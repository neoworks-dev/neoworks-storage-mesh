//! Repairing registered chunks while their owners' devices are off.
//!
//! Every repair round looks over the chunks this coordinator keeps
//! ([`crate::chunk_book`]). A piece is lost while its node is down, three
//! failed checks within ten minutes, or gone from the node list. A chunk
//! with lost pieces is repaired at once when it has no spare piece left, and
//! otherwise once its first lost piece has been down for the repair delay,
//! an hour by default, so a node that only reboots causes no repair.
//!
//! Repair works on sealed bytes only: it fetches as many pieces as the chunk
//! has data pieces, rebuilds the sealed chunk, cuts it again, and stores each
//! lost piece on a new node within the chunk's placement limits. Cutting is
//! deterministic, so a rebuilt piece is the lost one byte for byte and keeps
//! its id. The owner's client picks up the new locations the next time it
//! asks.
//!
//! A node that requires tokens is paid with a paid token the coordinator
//! signs for itself, and as much is taken out of the repair pool, which
//! holds what the failed node no longer earns ([`crate::earnings`]). That
//! value was paid for exactly the pieces the node held, so the owner pays
//! nothing twice. Repair traffic itself is not charged.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use mesh_client::erasure::{cut, rebuild};
use mesh_client::placement::{Spread, choose};
use mesh_client::tabs::new_tab_name;
use mesh_client::{NodeClient, NodeInfo, NodeList, NodeRequestError};
use mesh_core::chunk_registry::RegisteredPiece;
use mesh_core::node_list::{CoordinatorKey, CoordinatorList, ListedCoordinator};
use mesh_core::tabs::storage_cost;
use mesh_core::tokens::{MICROTOKENS_PER_TOKEN, MONTH_SECONDS, month_of};
use mesh_core::{NodeId, PieceId};
use reqwest::StatusCode;

use crate::chunk_book::{BookedChunk, ChunkBook};
use crate::clock::unix_now;
use crate::earnings::Earnings;
use crate::price_book::LatestPrices;
use crate::publish::LatestSnapshot;
use crate::registry::Registry;
use crate::settings::CoordinatorSettings;
use crate::token_issuer::TokenIssuer;

/// What a repair round needs.
pub struct Repairer {
    pub client: NodeClient,
    pub registry: Arc<Registry>,
    pub chunks: Arc<ChunkBook>,
    pub earnings: Arc<Earnings>,
    pub tokens: Arc<TokenIssuer>,
    pub latest: Arc<LatestSnapshot>,
    pub prices: Arc<LatestPrices>,
    /// The coordinator's own key, which its snapshots are checked with.
    pub own_key: CoordinatorKey,
}

/// A piece of a chunk that is lost for now.
struct LostPiece {
    position: usize,
    /// When its node went down; 0 for a node gone from the list.
    since: u64,
}

impl Repairer {
    /// Looks over every chunk still kept at `now` and repairs those due.
    pub async fn round(&self, now: u64, settings: &CoordinatorSettings) -> rusqlite::Result<()> {
        let Some(list) = self.usable_nodes().await? else {
            return Ok(());
        };
        let known: HashSet<NodeId> = list.nodes.iter().map(|node| node.id).collect();
        for chunk in self.chunks.live_chunks(now).await? {
            let lost = self.lost_pieces(&chunk, &known).await?;
            if is_due(&chunk, &lost, now, settings.repair_delay) {
                self.repair(&chunk, &lost, &list, now).await?;
            }
        }
        self.chunks.forget_ended(now).await
    }

    /// The nodes in the newest snapshot that are up right now, or `None`
    /// before the first snapshot.
    async fn usable_nodes(&self) -> rusqlite::Result<Option<NodeList>> {
        let Some(snapshot) = self.latest.read().await.clone() else {
            return Ok(None);
        };
        // Checking the snapshot looks at keys only, so this list of just
        // this coordinator needs no URL.
        let own = CoordinatorList {
            version: 0,
            coordinators: vec![ListedCoordinator {
                key: self.own_key,
                url: String::new(),
            }],
        };
        // The coordinator signed it itself, so this only fails if the key
        // changed under it; then there is nothing to place on until the next
        // snapshot.
        let Ok(mut list) = NodeList::from_snapshot(&snapshot, &own) else {
            return Ok(None);
        };
        // The snapshot can be an hour old; whether a node is down is known
        // from the latest checks.
        let mut up = Vec::with_capacity(list.nodes.len());
        for node in list.nodes {
            if self.registry.down_since(&node.id).await?.is_none() {
                up.push(node);
            }
        }
        list.nodes = up;
        Ok(Some(list))
    }

    /// The pieces of `chunk` on nodes that are down or not among `known`.
    async fn lost_pieces(
        &self,
        chunk: &BookedChunk,
        known: &HashSet<NodeId>,
    ) -> rusqlite::Result<Vec<LostPiece>> {
        let mut lost = Vec::new();
        for (position, piece) in chunk.pieces.iter().enumerate() {
            let down_since = self.registry.down_since(&piece.node).await?;
            let since = match down_since {
                Some(since) => since,
                None if known.contains(&piece.node) => continue,
                None => 0,
            };
            lost.push(LostPiece { position, since });
        }
        Ok(lost)
    }

    /// Rebuilds the `lost` pieces of `chunk` and stores them on nodes from
    /// `list`.
    async fn repair(
        &self,
        chunk: &BookedChunk,
        lost: &[LostPiece],
        list: &NodeList,
        now: u64,
    ) -> rusqlite::Result<()> {
        let lost_positions: HashSet<usize> = lost.iter().map(|piece| piece.position).collect();
        let survivors = self.fetch_survivors(chunk, &lost_positions, list).await;
        let sealed_len = chunk.sealed_len as usize;
        let Ok(sealed) = rebuild(&survivors, sealed_len, chunk.layout) else {
            eprintln!("repair: chunk {} can't be rebuilt", hex::encode(chunk.id));
            return Ok(());
        };
        let rebuilt = cut(&sealed, chunk.layout);
        let (holding, excluded) = split_nodes(chunk, &lost_positions);
        let spread = Spread {
            layout: chunk.layout,
            regions: usize::from(chunk.regions),
        };
        let targets = choose(list, lost.len(), &holding, &excluded, spread.limits());
        for (piece, node) in lost.iter().zip(targets) {
            let bytes = &rebuilt[piece.position];
            // Cutting is deterministic, so this holds unless the registration
            // and the pieces don't belong together.
            if PieceId::of(bytes) != chunk.pieces[piece.position].id {
                eprintln!("repair: a rebuilt piece does not match its registration");
                continue;
            }
            if !self.store(node, bytes, chunk.expires_at, now).await? {
                continue;
            }
            let moved = RegisteredPiece {
                id: chunk.pieces[piece.position].id,
                node: node.id,
            };
            self.chunks
                .move_piece(&chunk.id, piece.position, &moved)
                .await?;
        }
        Ok(())
    }

    /// As many of the chunk's pieces as it has data pieces, fetched from the
    /// nodes in `list` that hold them; every other piece is `None`.
    async fn fetch_survivors(
        &self,
        chunk: &BookedChunk,
        lost: &HashSet<usize>,
        list: &NodeList,
    ) -> Vec<Option<Vec<u8>>> {
        let mut pieces = vec![None; chunk.pieces.len()];
        let mut found = 0;
        for (position, piece) in chunk.pieces.iter().enumerate() {
            if found == chunk.layout.data() {
                break;
            }
            if lost.contains(&position) {
                continue;
            }
            let Some(url) = list.url_of(&piece.node) else {
                continue;
            };
            // `get` checks the bytes against the piece id, so a node can't
            // slip wrong bytes into the rebuild.
            if let Ok(Some(bytes)) = self.client.get(url, &piece.id).await {
                pieces[position] = Some(bytes);
                found += 1;
            }
        }
        pieces
    }

    /// Stores `bytes` on `node` until `expires_at` at most, paying it from
    /// the repair pool when it requires tokens, and whether it took them.
    async fn store(
        &self,
        node: &NodeInfo,
        bytes: &[u8],
        expires_at: u64,
        now: u64,
    ) -> rusqlite::Result<bool> {
        let id = PieceId::of(bytes);
        // Nodes grant up to 30 days by default; the owner renews from there.
        let lease = Duration::from_secs(expires_at.saturating_sub(now).min(MONTH_SECONDS));
        match self.client.put(&node.url, &id, bytes.to_vec(), lease).await {
            Ok(_) => return Ok(true),
            Err(NodeRequestError::Refused { status, .. }) if needs_a_tab(status) => {}
            Err(error) => {
                eprintln!("repair: {error}");
                return Ok(false);
            }
        }
        if !self.pay_tab(node, bytes.len() as u64, now).await? {
            return Ok(false);
        }
        // The client stores against the tab it just paid on this node.
        Ok(self
            .client
            .put(&node.url, &id, bytes.to_vec(), lease)
            .await
            .is_ok())
    }

    /// Opens a tab on `node` that covers `len` bytes for a month at its
    /// price, with tokens the coordinator signs for itself, and takes their
    /// value out of the repair pool. Whether the node took the tokens.
    async fn pay_tab(&self, node: &NodeInfo, len: u64, now: u64) -> rusqlite::Result<bool> {
        let Some(price) = self.price_of(&node.id).await else {
            eprintln!("repair: no price for node {}", node.id);
            return Ok(false);
        };
        let cost = storage_cost(len, price, MONTH_SECONDS);
        let count = cost.div_ceil(MICROTOKENS_PER_TOKEN).max(1);
        let mut tokens = Vec::new();
        for _ in 0..count {
            match self.tokens.mint_own(month_of(now)).await {
                Ok(token) => tokens.push(token),
                Err(error) => {
                    eprintln!("repair: making a token: {error}");
                    return Ok(false);
                }
            }
        }
        if let Err(error) = self
            .client
            .pay_tab(&node.url, &new_tab_name(), &tokens)
            .await
        {
            eprintln!("repair: {error}");
            return Ok(false);
        }
        let value = count * MICROTOKENS_PER_TOKEN;
        let taken = self.earnings.take_from_repair_pool(value).await?;
        // Data comes first: a repair the pool can't cover yet is still made,
        // and the coordinator carries the difference.
        if taken < value {
            eprintln!("repair: the repair pool covered {taken} of {value} millionths");
        }
        Ok(true)
    }

    /// The price of `node_id` in the newest snapshot and price table.
    async fn price_of(&self, node_id: &NodeId) -> Option<u64> {
        let snapshot = self.latest.read().await.clone()?;
        let table = self.prices.read().await.clone()?;
        let listed = snapshot
            .body
            .nodes
            .iter()
            .find(|listed| listed.record.body.node_id == *node_id)?;
        table.body.price_of_node(listed)
    }
}

/// Whether a node answering `status` to a store wants a (new) tab: it
/// requires tokens, or the tab it has is spent.
fn needs_a_tab(status: StatusCode) -> bool {
    status == StatusCode::PAYMENT_REQUIRED || status == StatusCode::INSUFFICIENT_STORAGE
}

/// Whether `chunk`, with `lost` pieces, is due for repair at `now`: at once
/// when it has no spare piece left, after `delay` otherwise. A chunk that
/// lost more than it can is beyond repair.
fn is_due(chunk: &BookedChunk, lost: &[LostPiece], now: u64, delay: Duration) -> bool {
    let parity = chunk.layout.parity();
    if lost.is_empty() || lost.len() > parity {
        return false;
    }
    let first_down = lost.iter().map(|piece| piece.since).min().unwrap_or(now);
    lost.len() == parity || now >= first_down + delay.as_secs()
}

/// The nodes of `chunk` that hold pieces not in `lost`, and those that hold
/// the lost ones, which must not get the rebuilt pieces back.
fn split_nodes(chunk: &BookedChunk, lost: &HashSet<usize>) -> (Vec<NodeId>, Vec<NodeId>) {
    let mut holding = Vec::new();
    let mut excluded = Vec::new();
    for (position, piece) in chunk.pieces.iter().enumerate() {
        if lost.contains(&position) {
            excluded.push(piece.node);
        } else {
            holding.push(piece.node);
        }
    }
    (holding, excluded)
}

/// Runs a repair round each repair interval, forever.
pub async fn repair_forever(repairer: Repairer, settings: CoordinatorSettings) {
    let mut timer = tokio::time::interval(settings.repair_interval);
    loop {
        timer.tick().await;
        // A failed round repairs what the next one finds still lost.
        if let Err(error) = repairer.round(unix_now(), &settings).await {
            eprintln!("repair: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use mesh_core::layout::Layout;

    use super::*;

    fn chunk_of(data: usize, parity: usize) -> BookedChunk {
        BookedChunk {
            id: [0; 32],
            layout: Layout::new(data, parity).unwrap(),
            regions: 1,
            sealed_len: 0,
            expires_at: 0,
            pieces: Vec::new(),
        }
    }

    fn lost_at(sinces: &[u64]) -> Vec<LostPiece> {
        sinces
            .iter()
            .enumerate()
            .map(|(position, &since)| LostPiece { position, since })
            .collect()
    }

    const HOUR: Duration = Duration::from_secs(60 * 60);

    #[test]
    fn a_chunk_with_a_spare_left_waits_an_hour() {
        let chunk = chunk_of(4, 2);

        assert!(!is_due(&chunk, &lost_at(&[1_000]), 1_000 + 3_599, HOUR));
        assert!(is_due(&chunk, &lost_at(&[1_000]), 1_000 + 3_600, HOUR));
    }

    #[test]
    fn a_chunk_without_a_spare_is_repaired_at_once() {
        assert!(is_due(
            &chunk_of(4, 2),
            &lost_at(&[1_000, 1_000]),
            1_000,
            HOUR
        ));
    }

    #[test]
    fn intact_chunks_and_lost_ones_are_left_alone() {
        let chunk = chunk_of(4, 2);

        assert!(!is_due(&chunk, &[], 10_000, HOUR));
        assert!(!is_due(&chunk, &lost_at(&[0, 0, 0]), 10_000, HOUR));
    }
}
