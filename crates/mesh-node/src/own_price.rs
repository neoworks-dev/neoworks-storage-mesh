//! What storing on this node costs, looked up at the coordinator it takes
//! tokens for.
//!
//! Nodes never set prices: the coordinator puts the node in a class and
//! publishes each class's price ([`mesh_core::prices`]). The node fetches the
//! signed node list and price table, finds itself in the list and keeps its
//! price for an hour; prices change once a day.

use std::time::{Duration, Instant};

use mesh_core::NodeId;
use mesh_core::node_list::NodeSnapshot;
use mesh_core::prices::{PRICES_ROUTE, PriceTable};
use mesh_core::protocol::NODE_LIST_ROUTE;
use mesh_core::signed::{Signed, SignedBody};
use tokio::sync::Mutex;

/// How long a price that was found is kept.
const PRICE_KEPT_FOR: Duration = Duration::from_secs(60 * 60);

/// How long the node waits before asking again after it found no price, so
/// that a node not listed yet does not ask on every upload.
const NO_PRICE_KEPT_FOR: Duration = Duration::from_secs(60);

/// The node's price and when it was looked up.
pub(crate) struct OwnPrice {
    coordinator: String,
    node_id: NodeId,
    http: reqwest::Client,
    last: Mutex<Option<(Instant, Option<u64>)>>,
}

impl OwnPrice {
    /// The price of `node_id` at `coordinator`, looked up when first asked.
    pub(crate) fn new(coordinator: String, node_id: NodeId, http: reqwest::Client) -> Self {
        OwnPrice {
            coordinator,
            node_id,
            http,
            last: Mutex::new(None),
        }
    }

    /// The node's price in tokens per TB and month, or `None` while the
    /// coordinator lists no price for it: it is not registered, not
    /// reliable enough for any class, or the coordinator can't be reached.
    pub(crate) async fn tokens_per_tb_month(&self) -> Option<u64> {
        // Held while looking up, so uploads arriving meanwhile wait for this
        // lookup instead of each starting their own.
        let mut last = self.last.lock().await;
        if let Some((looked_up_at, price)) = *last {
            let kept_for = match price {
                Some(_) => PRICE_KEPT_FOR,
                None => NO_PRICE_KEPT_FOR,
            };
            if looked_up_at.elapsed() < kept_for {
                return price;
            }
        }
        let price = self.look_up().await;
        *last = Some((Instant::now(), price));
        price
    }

    async fn look_up(&self) -> Option<u64> {
        let snapshot: Signed<NodeSnapshot> = self.fetch(NODE_LIST_ROUTE).await?;
        let table: Signed<PriceTable> = self.fetch(PRICES_ROUTE).await?;
        // The node trusts its coordinator already, since that is where it
        // spends tokens; the signatures only make sure both came from the
        // same coordinator unchanged.
        if snapshot.verify().is_err() || table.verify().is_err() || snapshot.signer != table.signer
        {
            return None;
        }
        let listed = snapshot
            .body
            .nodes
            .iter()
            .find(|node| node.record.body.node_id == self.node_id)?;
        table.body.price_of_node(listed)
    }

    async fn fetch<T: SignedBody>(&self, route: &str) -> Option<Signed<T>> {
        let url = format!("{}{route}", self.coordinator.trim_end_matches('/'));
        let response = self.http.get(url).send().await.ok()?;
        if !response.status().is_success() {
            return None;
        }
        let body = response.bytes().await.ok()?;
        serde_json::from_slice(&body).ok()
    }
}
