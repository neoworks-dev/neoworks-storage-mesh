//! What storage costs on each node, in tokens.
//!
//! Nodes never set prices. Nodes fall into classes by what coordinators
//! measured about them, and each class has one price. A coordinator works the
//! prices out once a day and signs them as a [`PriceTable`]:
//!
//! ```text
//! today's price = yesterday's price × (1 + 0.1 × (share of the class's space in use − 0.7)),
//!                 moved by at most 5% a day
//! ```
//!
//! A class more than 70% full gets more expensive, which steers new data to
//! cheaper classes and keeps room for repairs; an emptier one gets cheaper.
//! Operators decide how much space to offer at the published price, so a
//! class whose price is below its operators' costs shrinks until it fills
//! and its price rises.
//!
//! The calculation is [`next_table`], a function of the previous table and
//! the signed node list alone, so anyone can work out the same table from
//! the same inputs and check a coordinator's.
//!
//! One token is worth 0.1 euro cents. Prices are per TB of piece bytes and
//! month, since that is what nodes hold: a file stored as 4 + 2 pieces fills
//! 1.5 times its size. Users think in TB of their files, so prices start from
//! what a TB of file costs in the default layout.

use serde::{Deserialize, Serialize};

use crate::layout::Layout;
use crate::node_list::{ListedNode, NodeSnapshot};
use crate::signed::SignedBody;

/// The route a coordinator serves its newest signed price table at.
pub const PRICES_ROUTE: &str = "/prices";

/// Seconds in a price day; a table is valid for the Unix day it names.
pub const DAY_SECONDS: u64 = 24 * 60 * 60;

/// The share of a class's space in use that keeps its price where it is, in
/// thousandths.
const TARGET_USE_PERMILLE: i64 = 700;

/// The most a price moves in one day, in thousandths.
const MAX_DAILY_CHANGE_PERMILLE: i64 = 50;

/// A group of nodes of similar speed and reliability, which share a price.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeClass {
    /// Reliable enough to hold data, at any speed.
    Archive,
    /// Up almost always and fast enough to read from at home speeds.
    Standard,
    /// Up nearly always on a data centre connection.
    Fast,
}

/// What a node needs to be in a class.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClassRule {
    pub class: NodeClass,
    /// The least uptime, in thousandths.
    pub min_uptime_permille: u16,
    /// The least measured bandwidth, in bits per second. A node not measured
    /// yet counts as 0.
    pub min_bits_per_second: u64,
}

/// One class's price.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClassPrice {
    pub class: NodeClass,
    /// Tokens per TB of piece bytes and month. At 0.1 cents a token, 1,000
    /// is 1 euro.
    pub tokens_per_tb_month: u64,
}

/// The prices of one day, signed by a coordinator.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceTable {
    /// The Unix day the prices hold for: Unix seconds divided by
    /// [`DAY_SECONDS`].
    pub day: u64,
    /// The classes from the most demanding to the least; a node belongs to
    /// the first one it meets.
    pub rules: Vec<ClassRule>,
    pub prices: Vec<ClassPrice>,
}

impl SignedBody for PriceTable {
    const CONTEXT: &'static str = "neoworks-mesh price table v1";
}

impl PriceTable {
    /// The table prices start from: 8 euros per TB of file and month on fast
    /// nodes in the default 4 + 2 layout, which is what NeoWorks charges on
    /// its servers, and half and a quarter of that on slower ones. The
    /// thresholds and the lower prices are a starting guess that demand
    /// corrects.
    pub fn starting(day: u64) -> Self {
        PriceTable {
            day,
            rules: vec![
                ClassRule {
                    class: NodeClass::Fast,
                    min_uptime_permille: 999,
                    min_bits_per_second: 1_000_000_000,
                },
                ClassRule {
                    class: NodeClass::Standard,
                    min_uptime_permille: 990,
                    min_bits_per_second: 50_000_000,
                },
                ClassRule {
                    class: NodeClass::Archive,
                    min_uptime_permille: 950,
                    min_bits_per_second: 0,
                },
            ],
            prices: vec![
                ClassPrice {
                    class: NodeClass::Fast,
                    tokens_per_tb_month: per_piece_tb(8_000),
                },
                ClassPrice {
                    class: NodeClass::Standard,
                    tokens_per_tb_month: per_piece_tb(4_000),
                },
                ClassPrice {
                    class: NodeClass::Archive,
                    tokens_per_tb_month: per_piece_tb(2_000),
                },
            ],
        }
    }

    /// The class `node` belongs to, or `None` when it is not reliable enough
    /// for any.
    pub fn class_of(&self, node: &ListedNode) -> Option<NodeClass> {
        let bits_per_second = node.bandwidth_bits_per_second.unwrap_or(0);
        self.rules
            .iter()
            .find(|rule| {
                node.uptime_permille >= rule.min_uptime_permille
                    && bits_per_second >= rule.min_bits_per_second
            })
            .map(|rule| rule.class)
    }

    /// What `class` costs, in tokens per TB and month.
    pub fn price_of(&self, class: NodeClass) -> Option<u64> {
        self.prices
            .iter()
            .find(|price| price.class == class)
            .map(|price| price.tokens_per_tb_month)
    }

    /// What storing on `node` costs, in tokens per TB and month, or `None`
    /// when the node is in no class.
    pub fn price_of_node(&self, node: &ListedNode) -> Option<u64> {
        self.price_of(self.class_of(node)?)
    }
}

/// The table for `day` that follows `previous`, from how full each class is
/// in `snapshot`.
///
/// A class without nodes, or whose nodes report no space, keeps its price.
/// However many days lie between `previous` and `day`, prices move one step,
/// so a coordinator that was down for a while does not jump them.
pub fn next_table(previous: &PriceTable, snapshot: &NodeSnapshot, day: u64) -> PriceTable {
    let prices = previous
        .prices
        .iter()
        .map(|price| ClassPrice {
            class: price.class,
            tokens_per_tb_month: match use_permille(previous, snapshot, price.class) {
                Some(used) => next_price(price.tokens_per_tb_month, used),
                None => price.tokens_per_tb_month,
            },
        })
        .collect();
    PriceTable {
        day,
        rules: previous.rules.clone(),
        prices,
    }
}

/// The share of the space the nodes of `class` offer that is in use, in
/// thousandths, or `None` when they offer none.
fn use_permille(table: &PriceTable, snapshot: &NodeSnapshot, class: NodeClass) -> Option<i64> {
    let mut used: u128 = 0;
    let mut offered: u128 = 0;
    for node in &snapshot.nodes {
        if table.class_of(node) != Some(class) {
            continue;
        }
        let capacity = node.record.body.capacity_bytes;
        // A node can't fill more than it offers; reporting more would only
        // push its class's price up.
        used += u128::from(node.used_bytes.unwrap_or(0).min(capacity));
        offered += u128::from(capacity);
    }
    if offered == 0 {
        return None;
    }
    // At most 1000, since `used` never exceeds `offered`.
    Some((used * 1000 / offered) as i64)
}

/// `price` moved by how far `used_permille` is from the target.
/// The price per TB of pieces at which a TB of file in the default layout
/// costs `tokens_per_file_tb`, rounded to the nearest token.
const fn per_piece_tb(tokens_per_file_tb: u64) -> u64 {
    let data = Layout::DEFAULT.data() as u64;
    let total = Layout::DEFAULT.total() as u64;
    (tokens_per_file_tb * data + total / 2) / total
}

fn next_price(price: u64, used_permille: i64) -> u64 {
    let change = ((used_permille - TARGET_USE_PERMILLE) / 10)
        .clamp(-MAX_DAILY_CHANGE_PERMILLE, MAX_DAILY_CHANGE_PERMILLE);
    // `change` is within ±50, so the factor is between 950 and 1050.
    let factor = (1000 + change) as u64;
    // A price of one token keeps a class able to rise again; at 0 it would
    // stay there.
    (price.saturating_mul(factor) / 1000).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeId;
    use crate::node_list::NodeRecord;
    use crate::signed::{Signed, SigningKey};

    fn node(uptime_permille: u16, bits_per_second: Option<u64>, used: u64) -> ListedNode {
        let key = SigningKey::from_bytes(&[1; 32]);
        let record = NodeRecord {
            node_id: NodeId::of(&key.verifying_key().to_bytes()),
            url: "http://203.0.113.7:7001".into(),
            operator: "alice".into(),
            network: "AS3320".into(),
            region: "de".into(),
            capacity_bytes: 1_000,
            signed_at: 1,
        };
        ListedNode {
            record: Signed::sign(&key, record),
            uptime_permille,
            audit_pass_permille: None,
            bandwidth_bits_per_second: bits_per_second,
            used_bytes: Some(used),
            takes_allowance: false,
            measured_network: None,
            measured_country: None,
            operator_id: None,
            down: false,
        }
    }

    fn snapshot(nodes: Vec<ListedNode>) -> NodeSnapshot {
        NodeSnapshot {
            published_at: 1,
            nodes,
            daily_loss_per_million: None,
        }
    }

    #[test]
    fn a_node_belongs_to_the_most_demanding_class_it_meets() {
        let table = PriceTable::starting(0);

        assert_eq!(
            table.class_of(&node(1000, Some(2_000_000_000), 0)),
            Some(NodeClass::Fast)
        );
        assert_eq!(
            table.class_of(&node(1000, Some(100_000_000), 0)),
            Some(NodeClass::Standard)
        );
        // Not measured yet: counted as slow.
        assert_eq!(
            table.class_of(&node(995, None, 0)),
            Some(NodeClass::Archive)
        );
        assert_eq!(table.class_of(&node(900, Some(2_000_000_000), 0)), None);
    }

    #[test]
    fn prices_follow_how_full_a_class_is() {
        assert_eq!(next_price(8_000, 700), 8_000);
        assert_eq!(next_price(8_000, 900), 8_160);
        assert_eq!(next_price(8_000, 500), 7_840);
        // Never more than 5% a day.
        assert_eq!(next_price(8_000, 1_000), 8_240);
        assert_eq!(next_price(8_000, 0), 7_600);
        assert_eq!(next_price(1, 0), 1);
    }

    #[test]
    fn the_next_table_moves_each_class_by_its_own_use() {
        let previous = PriceTable::starting(10);
        let fast_and_full = node(1000, Some(2_000_000_000), 1_000);
        let archive_and_empty = node(960, None, 0);

        let next = next_table(
            &previous,
            &snapshot(vec![fast_and_full, archive_and_empty]),
            11,
        );

        assert_eq!(next.day, 11);
        assert_eq!(next.price_of(NodeClass::Fast), Some(5_492));
        // No standard nodes: the price stays.
        assert_eq!(next.price_of(NodeClass::Standard), Some(2_667));
        assert_eq!(next.price_of(NodeClass::Archive), Some(1_266));
    }

    #[test]
    fn a_tb_of_file_in_the_default_layout_starts_at_8_euros_on_fast_nodes() {
        let table = PriceTable::starting(0);

        assert_eq!(table.price_of(NodeClass::Fast), Some(5_333));
        assert_eq!(table.price_of(NodeClass::Standard), Some(2_667));
        assert_eq!(table.price_of(NodeClass::Archive), Some(1_333));
    }

    #[test]
    fn reporting_more_than_the_capacity_counts_as_full() {
        let previous = PriceTable::starting(0);
        let overfull = node(1000, Some(2_000_000_000), u64::MAX);

        let next = next_table(&previous, &snapshot(vec![overfull]), 1);

        assert_eq!(next.price_of(NodeClass::Fast), Some(5_492));
    }
}
