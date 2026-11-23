//! How many messages each sending address left in each contact mailbox this
//! week, so that one sender can't fill a mailbox, or the node, alone.
//!
//! A node can't tell who a contact message is from, only which address sent
//! it. It takes at most [`MAX_MESSAGES_PER_SENDER`] per contact mailbox and
//! week from each address. An IPv6 /64 counts as one address, since one
//! subscriber usually holds a whole /64 and would otherwise get 2^64 fresh
//! starts.
//!
//! The counts live in memory, so a restart of the node forgets them. Counts
//! of weeks before the current one are dropped as the week turns. Only
//! messages the node stored are counted, so there are never more counts
//! than contact messages held.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr};

use mesh_core::mailbox::MAX_MESSAGES_PER_SENDER;

/// Messages per contact mailbox address and sending address, for one week.
#[derive(Default)]
pub(crate) struct ContactSenders {
    week: u64,
    counts: HashMap<([u8; 32], IpAddr), u32>,
}

impl ContactSenders {
    /// Whether `sender` may leave another message in the contact mailbox
    /// `address` in `week`, the node's current week.
    pub(crate) fn has_room(&mut self, address: &[u8; 32], sender: IpAddr, week: u64) -> bool {
        if week != self.week {
            self.week = week;
            self.counts.clear();
        }
        let count = self.counts.get(&(*address, counted(sender)));
        count.is_none_or(|&count| count < MAX_MESSAGES_PER_SENDER)
    }

    /// Counts a message `sender` left in the contact mailbox `address`.
    pub(crate) fn record(&mut self, address: &[u8; 32], sender: IpAddr) {
        *self.counts.entry((*address, counted(sender))).or_insert(0) += 1;
    }
}

/// The address `sender` is counted under: itself for IPv4, its /64 for IPv6.
fn counted(sender: IpAddr) -> IpAddr {
    match sender {
        IpAddr::V4(_) => sender,
        IpAddr::V6(address) => {
            let network = address.to_bits() & !u128::from(u64::MAX);
            IpAddr::V6(Ipv6Addr::from_bits(network))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAILBOX: [u8; 32] = [1; 32];

    /// How many of `times` messages from `sender` to `mailbox` in `week` are
    /// let in, counting each one let in as stored.
    fn admitted(
        senders: &mut ContactSenders,
        mailbox: [u8; 32],
        sender: &str,
        week: u64,
        times: u32,
    ) -> u32 {
        let sender: IpAddr = sender.parse().unwrap();
        let mut admitted = 0;
        for _ in 0..times {
            if senders.has_room(&mailbox, sender, week) {
                senders.record(&mailbox, sender);
                admitted += 1;
            }
        }
        admitted
    }

    #[test]
    fn one_sender_gets_its_share_per_mailbox_and_week() {
        let mut senders = ContactSenders::default();

        assert_eq!(admitted(&mut senders, MAILBOX, "192.0.2.1", 5, 20), 16);
        assert_eq!(admitted(&mut senders, MAILBOX, "192.0.2.2", 5, 1), 1);
        assert_eq!(admitted(&mut senders, [2; 32], "192.0.2.1", 5, 1), 1);
        assert_eq!(admitted(&mut senders, MAILBOX, "192.0.2.1", 6, 1), 1);
    }

    #[test]
    fn a_message_not_stored_uses_up_nothing() {
        let mut senders = ContactSenders::default();
        let sender: IpAddr = "192.0.2.1".parse().unwrap();

        for _ in 0..20 {
            assert!(senders.has_room(&MAILBOX, sender, 5));
        }
    }

    #[test]
    fn an_ipv6_network_counts_as_one_sender() {
        let mut senders = ContactSenders::default();

        assert_eq!(admitted(&mut senders, MAILBOX, "2001:db8::1", 5, 10), 10);
        assert_eq!(
            admitted(&mut senders, MAILBOX, "2001:db8::ffff:2", 5, 10),
            6
        );
        assert_eq!(admitted(&mut senders, MAILBOX, "2001:db8:0:1::1", 5, 1), 1);
    }
}
