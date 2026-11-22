//! Choosing a file's layout from what it is worth to the app storing it.
//!
//! An app says, as plain values, how much loss it can tolerate, how fast the
//! file must read and how far apart it must be stored ([`Worth`]). The client
//! then takes the cheapest layout that meets all three and that the node
//! list has enough independent nodes for, and says what it costs a month
//! before anything is stored.
//!
//! The chance of losing a chunk comes from the loss rate the coordinator
//! publishes ([`loss_per_year`]). Cost per stored byte is
//! `(data + parity) / data`, so more data pieces buy more safety for the same
//! price; fewer data pieces read faster, since a read waits on fewer nodes.

use mesh_core::layout::{Layout, MAX_PIECES, MIN_DATA_PIECES, loss_per_year};
use mesh_core::tabs::storage_cost;
use mesh_core::tokens::MONTH_SECONDS;

use crate::node_list::NodeList;
use crate::placement::{Spread, can_place};

/// The chance per day that a node is lost for good, assumed while the
/// coordinator has watched too little to publish one: 1 in 1,000, the
/// figure the design was worked out with.
pub const ASSUMED_DAILY_LOSS: f64 = 0.001;

/// What a file or a collection is worth to the app storing it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Worth {
    /// The highest chance per year of losing it the app accepts, such as
    /// 1e-9 for "keep forever".
    pub max_loss_per_year: f64,
    /// The most data pieces a read may wait on. Each read fetches that many
    /// pieces, so 2 suits streaming, while 16 or more suits an archive.
    pub max_data_pieces: usize,
    /// The fewest regions the pieces must spread over. Losing a whole region
    /// never loses the file, whatever this is.
    pub min_regions: usize,
}

/// A layout and what it comes to.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LayoutChoice {
    pub spread: Spread,
    /// The chance of losing a chunk within a year.
    pub loss_per_year: f64,
}

/// No layout meets what a file is worth on the nodes there are.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("no layout of at most {MAX_PIECES} pieces meets these settings on the nodes in the list")]
pub struct NoLayout;

/// The cheapest layout for `worth` that `list` can place, with `daily_loss`
/// the published chance that a node is lost on a given day, in millionths,
/// or `None` while the coordinator hasn't published one.
///
/// Among layouts of equal cost the one with the fewest pieces wins, since
/// every piece is a request on every read and renewal.
pub fn choose_layout(
    worth: &Worth,
    list: &NodeList,
    daily_loss_per_million: Option<u32>,
) -> Result<LayoutChoice, NoLayout> {
    let daily_loss = daily_loss_per_million.map_or(ASSUMED_DAILY_LOSS, |per_million| {
        f64::from(per_million) / 1e6
    });
    let max_data = worth.max_data_pieces.min(MAX_PIECES);
    let mut best: Option<LayoutChoice> = None;
    for data in MIN_DATA_PIECES..=max_data {
        for parity in 0..=MAX_PIECES - data {
            // `data` and `parity` stay within `MAX_PIECES` by the loops.
            let Ok(layout) = Layout::new(data, parity) else {
                continue;
            };
            // Fewer pieces than regions can't fill them all, however they
            // are placed; the placement limits only cap pieces per region.
            if layout.total() < worth.min_regions {
                continue;
            }
            let loss = loss_per_year(layout, daily_loss);
            if loss > worth.max_loss_per_year {
                continue;
            }
            let spread = Spread {
                layout,
                regions: worth.min_regions,
            };
            if is_better(spread, best) && can_place(list, layout, spread.limits()) {
                best = Some(LayoutChoice {
                    spread,
                    loss_per_year: loss,
                });
            }
            // More parity only costs more once the target is met.
            break;
        }
    }
    best.ok_or(NoLayout)
}

/// Whether `spread` beats the best choice so far: cheaper, or as cheap with
/// fewer pieces.
fn is_better(spread: Spread, best: Option<LayoutChoice>) -> bool {
    let Some(best) = best else {
        return true;
    };
    let (layout, other) = (spread.layout, best.spread.layout);
    // Cross-multiplied, so the comparison of total / data is exact.
    let cost = layout.total() * other.data();
    let other_cost = other.total() * layout.data();
    cost < other_cost || (cost == other_cost && layout.total() < other.total())
}

/// What keeping `file_bytes` in `layout` for a month costs at
/// `tokens_per_tb_month`, the price of the nodes it goes to, in millionths
/// of a token. Shown to the user before storing.
pub fn monthly_cost(file_bytes: u64, layout: Layout, tokens_per_tb_month: u64) -> u64 {
    // Pieces take `total / data` of the file, rounded up per piece; the
    // rounding is a few bytes per chunk and left out here.
    let stored_bytes = file_bytes.div_ceil(layout.data() as u64) * layout.total() as u64;
    storage_cost(stored_bytes, tokens_per_tb_month, MONTH_SECONDS)
}

#[cfg(test)]
mod tests {
    use mesh_core::NodeId;

    use super::*;
    use crate::node_list::NodeInfo;

    /// `count` nodes of distinct operators, networks and subnets, spread
    /// evenly over `regions` regions.
    fn network(count: usize, regions: usize) -> NodeList {
        let nodes = (0..count)
            .map(|index| NodeInfo {
                id: NodeId::of(&[index as u8; 32]),
                url: format!("http://10.{}.{}.1:7001", index / 256, index % 256),
                operator: format!("operator-{index}"),
                network: format!("network-{index}"),
                region: Some(format!("region-{}", index % regions)),
                weight: 1.0,
            })
            .collect();
        NodeList { nodes }
    }

    fn worth(max_loss_per_year: f64, max_data_pieces: usize, min_regions: usize) -> Worth {
        Worth {
            max_loss_per_year,
            max_data_pieces,
            min_regions,
        }
    }

    #[test]
    fn a_large_network_gets_a_wide_cheap_layout() {
        let list = network(200, 10);

        let choice = choose_layout(&worth(1e-6, 32, 1), &list, Some(1_000)).unwrap();

        // Wider than 4 + 2 and cheaper, and still within the target.
        assert!(choice.spread.layout.data() > 4);
        assert!(choice.spread.layout.overhead() < 1.5);
        assert!(choice.loss_per_year <= 1e-6);
    }

    #[test]
    fn a_small_network_limits_how_wide_a_layout_can_be() {
        let list = network(8, 4);

        let choice = choose_layout(&worth(1e-4, 32, 1), &list, Some(1_000)).unwrap();

        assert!(choice.spread.layout.total() <= 8);
    }

    #[test]
    fn streaming_reads_from_few_pieces() {
        let list = network(200, 10);

        let choice = choose_layout(&worth(1e-6, 2, 1), &list, Some(1_000)).unwrap();

        assert_eq!(choice.spread.layout.data(), 2);
    }

    #[test]
    fn reading_from_one_node_keeps_whole_copies() {
        let list = network(200, 10);

        let choice = choose_layout(&worth(1e-6, 1, 1), &list, Some(1_000)).unwrap();

        assert_eq!(choice.spread.layout.data(), 1);
    }

    #[test]
    fn copies_never_count_as_spread_over_more_regions_than_they_are() {
        let list = network(200, 10);

        let choice = choose_layout(&worth(1e-2, 1, 3), &list, Some(1_000)).unwrap();

        assert!(choice.spread.layout.total() >= 3);
    }

    #[test]
    fn more_regions_than_the_network_has_can_not_be_met() {
        let list = network(6, 2);

        let result = choose_layout(&worth(1e-3, 4, 3), &list, Some(1_000));

        assert_eq!(result, Err(NoLayout));
    }

    #[test]
    fn a_month_costs_the_layouts_overhead_at_the_price() {
        let tb = 1_000_000_000_000;

        assert_eq!(monthly_cost(tb, Layout::DEFAULT, 8_000), 12_000_000_000);
    }
}
