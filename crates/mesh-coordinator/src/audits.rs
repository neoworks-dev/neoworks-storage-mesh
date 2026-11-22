//! Auditing nodes with pieces of the coordinator's own.
//!
//! A node that answers `/identity` but threw its pieces away would look
//! perfect by uptime alone. So the coordinator keeps a small piece of random
//! bytes on every node and challenges it each audit round, the way a client
//! audits its files: the node returns a few random segments with Merkle
//! proofs, which only a node holding the bytes can do. The share of
//! challenges a node passes goes into the node list, and it decides whether
//! the node earns ([`crate::earnings`]).
//!
//! Clients could report their own audits instead, but that would tell the
//! coordinator which nodes hold whose pieces, and a client could lie about
//! nodes it dislikes.
//!
//! A node that requires tokens is paid like any client pays it: the
//! coordinator opens a tab with a paid token it signs for itself
//! ([`TokenIssuer::mint_own`]), and the node's operator earns that token.
//! A node that does not take the coordinator's tokens, or refuses the piece,
//! is never audited, and a node never audited earns nothing.
//!
//! A challenge the node fails to answer, for whatever reason, counts as
//! failed. Otherwise a node that lost the piece could time out on purpose
//! whenever it is challenged, while still answering the uptime checks.

use std::sync::Arc;

use futures::future::join_all;
use mesh_core::merkle::{self, SegmentProof};
use mesh_core::node_list::NodeRecord;
use mesh_core::protocol::{ChallengeRequest, ChallengeResponse, LeaseResponse, challenge_path};
use mesh_core::tabs::{OpenTabRequest, tab_path};
use mesh_core::tokens::month_of;
use mesh_core::{NodeId, PieceId};
use reqwest::StatusCode;

use crate::audit_log::{AuditLog, AuditPiece};
use crate::clock::unix_now;
use crate::registry::Registry;
use crate::settings::CoordinatorSettings;
use crate::token_issuer::TokenIssuer;

/// The size of an audit piece: 16 segments, few enough bytes that keeping
/// one on every node costs next to nothing.
const AUDIT_PIECE_BYTES: usize = 16 * merkle::SEGMENT_BYTES;

/// Segments asked for per challenge. A node that kept half of the piece
/// passes with a chance of 1/16, and challenges repeat every round.
const SEGMENTS_PER_CHALLENGE: usize = 4;

/// The lease asked for on nodes that grant leases freely: a week, well under
/// the 30 days nodes grant by default.
const FREE_LEASE_SECONDS: u64 = 7 * 24 * 60 * 60;

/// A piece whose lease ends within this is replaced by a new one, so a node
/// is never left without a piece to be challenged for.
const REPLACE_BEFORE_SECONDS: u64 = 24 * 60 * 60;

/// What an audit round needs.
pub struct Auditor {
    pub http: reqwest::Client,
    pub registry: Arc<Registry>,
    pub log: Arc<AuditLog>,
    pub tokens: Arc<TokenIssuer>,
}

impl Auditor {
    /// Audits every registered node once at `now`, placing new pieces where
    /// needed.
    pub async fn round(&self, now: u64, settings: &CoordinatorSettings) -> rusqlite::Result<()> {
        let records = self.registry.records().await?;
        let audits = records
            .iter()
            .map(|record| self.audit_node(&record.body, now));
        for result in join_all(audits).await {
            result?;
        }
        // Results older than the window no longer count anywhere.
        let before = now.saturating_sub(settings.uptime_window.as_secs());
        self.log.forget(now, before).await
    }

    /// Challenges the node `record` names for its audit piece, after placing
    /// a new one if it has none that lasts.
    async fn audit_node(&self, record: &NodeRecord, now: u64) -> rusqlite::Result<()> {
        let held = self.log.newest_piece(&record.node_id, now).await?;
        let lasts = held
            .as_ref()
            .is_some_and(|piece| piece.expires_at > now + REPLACE_BEFORE_SECONDS);
        let piece = if lasts {
            held
        } else {
            self.place(record, now).await?.or(held)
        };
        let Some(piece) = piece else {
            return Ok(());
        };
        let passed = challenge(&self.http, &record.url, &piece).await;
        self.log.record(&record.node_id, now, passed).await
    }

    /// Stores a new audit piece on the node `record` names and returns it, or
    /// `None` when the node did not take it.
    async fn place(&self, record: &NodeRecord, now: u64) -> rusqlite::Result<Option<AuditPiece>> {
        let mut bytes = vec![0u8; AUDIT_PIECE_BYTES];
        getrandom::fill(&mut bytes).expect("the operating system's random source works");
        let stored = self.store(&record.url, &bytes, now).await;
        let Some(expires_at) = stored else {
            eprintln!(
                "audits: node {} did not take an audit piece",
                record.node_id
            );
            return Ok(None);
        };
        let piece = AuditPiece {
            id: PieceId::of(&bytes),
            merkle_root: merkle::root_of(&bytes),
            len: bytes.len() as u64,
            expires_at,
        };
        self.log.add_piece(&record.node_id, &piece).await?;
        Ok(Some(piece))
    }

    /// Stores `bytes` on the node at `url` and returns when the lease ends:
    /// under a free lease, or against a tab paid with one of the
    /// coordinator's own tokens when the node requires tokens.
    async fn store(&self, url: &str, bytes: &[u8], now: u64) -> Option<u64> {
        let id = PieceId::of(bytes);
        let free = format!("{}?lease_seconds={FREE_LEASE_SECONDS}", piece_url(url, &id));
        let response = self.http.put(free).body(bytes.to_vec()).send().await.ok()?;
        if response.status() != StatusCode::PAYMENT_REQUIRED {
            return lease_end(response).await;
        }
        let tab = self.open_tab(url, now).await?;
        let paid = format!("{}?tab={tab}", piece_url(url, &id));
        let response = self.http.put(paid).body(bytes.to_vec()).send().await.ok()?;
        lease_end(response).await
    }

    /// Opens a tab on the node at `url`, paid with one token of the
    /// coordinator's own, and returns its name.
    ///
    /// One token is far more than a piece of 64 KiB costs for a month at any
    /// price, and the rest of the balance is simply not used.
    async fn open_tab(&self, url: &str, now: u64) -> Option<String> {
        let token = match self.tokens.mint_own(month_of(now)).await {
            Ok(token) => token,
            Err(error) => {
                eprintln!("audits: making a token: {error}");
                return None;
            }
        };
        let mut name_bytes = [0u8; 16];
        getrandom::fill(&mut name_bytes).expect("the operating system's random source works");
        let name = hex::encode(name_bytes);
        let request = OpenTabRequest {
            tokens: vec![token],
        };
        // A token is plain integers and bytes, which always serialize.
        let body = serde_json::to_vec(&request).expect("a tab request serializes");
        let tab_url = format!("{}{}", url.trim_end_matches('/'), tab_path(&name));
        let response = self
            .http
            .post(tab_url)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .ok()?;
        response.status().is_success().then_some(name)
    }
}

/// The URL of the piece `id` on the node at `url`.
fn piece_url(url: &str, id: &PieceId) -> String {
    format!(
        "{}{}",
        url.trim_end_matches('/'),
        mesh_core::protocol::piece_path(id)
    )
}

/// When the lease in a successful store answer ends, or `None` when the node
/// refused.
async fn lease_end(response: reqwest::Response) -> Option<u64> {
    if !response.status().is_success() {
        return None;
    }
    let body = response.bytes().await.ok()?;
    let lease: LeaseResponse = serde_json::from_slice(&body).ok()?;
    Some(lease.expires_at)
}

/// Whether the node at `url` proves it holds `piece`, for segments it can't
/// know in advance.
async fn challenge(http: &reqwest::Client, url: &str, piece: &AuditPiece) -> bool {
    let segments = random_segments(merkle::segment_count(piece.len as usize));
    let request = ChallengeRequest {
        segments: segments.clone(),
    };
    // A list of integers always serializes.
    let body = serde_json::to_vec(&request).expect("a challenge serializes");
    let challenge_url = format!("{}{}", url.trim_end_matches('/'), challenge_path(&piece.id));
    let Ok(response) = http
        .post(challenge_url)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
    else {
        return false;
    };
    if !response.status().is_success() {
        return false;
    }
    let Ok(body) = response.bytes().await else {
        return false;
    };
    let Ok(answer) = serde_json::from_slice::<ChallengeResponse>(&body) else {
        return false;
    };
    proofs_hold(&answer.proofs, &segments, piece)
}

/// Whether `proofs` answer exactly `segments`, in order, and lead to the
/// root of `piece`. A proof for a segment not asked for proves nothing: the
/// node could keep that one segment and hand it out every time.
fn proofs_hold(proofs: &[SegmentProof], segments: &[u32], piece: &AuditPiece) -> bool {
    proofs.len() == segments.len()
        && proofs
            .iter()
            .zip(segments)
            .all(|(proof, &asked)| proof.index == asked && proof.leads_to(&piece.merkle_root))
}

/// [`SEGMENTS_PER_CHALLENGE`] random segment numbers below `segment_count`.
/// Repeats are possible and harmless: they only make a challenge ask for
/// fewer distinct segments.
fn random_segments(segment_count: u32) -> Vec<u32> {
    let mut random = [0u8; 4 * SEGMENTS_PER_CHALLENGE];
    getrandom::fill(&mut random).expect("the operating system's random source works");
    random
        .chunks_exact(4)
        // The remainder favours low numbers by at most count / 2^32, far too
        // little for a node to make use of.
        .map(|bytes| u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) % segment_count)
        .collect()
}

/// Runs an audit round each audit interval, forever.
pub async fn audit_forever(auditor: Auditor, settings: CoordinatorSettings) {
    let mut timer = tokio::time::interval(settings.audit_interval);
    loop {
        timer.tick().await;
        // A failed round leaves the results it had; the next one carries on.
        if let Err(error) = auditor.round(unix_now(), &settings).await {
            eprintln!("audits: {error}");
        }
    }
}

/// Whether `node_id` passed enough of its audits since `since` to earn,
/// which needs at least one audit: a node that never took an audit piece
/// would otherwise dodge them.
pub async fn passes_audits(
    log: &AuditLog,
    node_id: &NodeId,
    since: u64,
    needed_permille: u16,
) -> rusqlite::Result<bool> {
    let share = log.pass_permille(node_id, since).await?;
    Ok(share.is_some_and(|permille| permille >= needed_permille))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn piece_of(bytes: &[u8]) -> AuditPiece {
        AuditPiece {
            id: PieceId::of(bytes),
            merkle_root: merkle::root_of(bytes),
            len: bytes.len() as u64,
            expires_at: 0,
        }
    }

    #[test]
    fn segments_fall_inside_the_piece() {
        for _ in 0..100 {
            let segments = random_segments(16);
            assert_eq!(segments.len(), SEGMENTS_PER_CHALLENGE);
            assert!(segments.iter().all(|&segment| segment < 16));
        }
    }

    #[test]
    fn only_proofs_of_the_segments_asked_for_pass() {
        let bytes: Vec<u8> = (0..AUDIT_PIECE_BYTES).map(|index| index as u8).collect();
        let piece = piece_of(&bytes);
        let proof = |index| merkle::prove(&bytes, index).unwrap();

        assert!(proofs_hold(&[proof(2), proof(9)], &[2, 9], &piece));
        assert!(!proofs_hold(&[proof(2), proof(2)], &[2, 9], &piece));
        assert!(!proofs_hold(&[proof(2)], &[2, 9], &piece));
        let other = piece_of(&vec![7; AUDIT_PIECE_BYTES]);
        assert!(!proofs_hold(&[proof(2)], &[2], &other));
    }
}
