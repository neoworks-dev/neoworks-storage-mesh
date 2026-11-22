//! Limits on failed sign-ins, so nobody can guess passwords online at the
//! speed of the network.
//!
//! The coordinator counts failed sign-ins in a row per email and per address.
//! After [`FREE_FAILURES`] of them it refuses further attempts for a while,
//! twice as long after each further failure, up to [`MAX_LOCK_SECONDS`].
//! Counting per email stops a guesser who switches addresses; counting per
//! address stops one who tries a common password on many emails.
//!
//! Unknown emails are counted exactly like known ones. If they were not,
//! a guesser could tell which emails have accounts by which ones get locked.
//!
//! The counts live in memory, so a restart of the coordinator forgets them.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr};

use mesh_core::sign_in::normalize_email;

/// Failures in a row that are answered as usual. Enough for someone who
/// mistypes their password a few times.
const FREE_FAILURES: u32 = 5;

/// How long the first lock lasts.
const FIRST_LOCK_SECONDS: u64 = 60;

/// The longest lock. It holds a guesser to about 24 guesses a day per email,
/// and keeps the owner of an email that a stranger keeps locking from
/// waiting longer than an hour.
const MAX_LOCK_SECONDS: u64 = 60 * 60;

/// After a day without a failure, the failures before no longer count.
const FORGET_AFTER_SECONDS: u64 = 24 * 60 * 60;

/// Failed sign-ins in a row per email and per address.
pub struct FailedSignIns {
    failures: HashMap<Counted, Failures>,
    /// Dropping forgotten counts walks the whole map, so it happens at most
    /// once every [`FORGET_AFTER_SECONDS`] rather than on every failure.
    next_cleanup_at: u64,
}

/// What failures are counted against.
#[derive(Clone, PartialEq, Eq, Hash)]
enum Counted {
    /// An email as normalized for accounts, whether it has one or not.
    Email(String),
    /// An IPv4 address, or the /64 network of an IPv6 address: one IPv6
    /// subscriber usually holds a whole /64, so counting single addresses
    /// would give them 2^64 fresh starts.
    Address(IpAddr),
}

/// Failures in a row against one email or address.
#[derive(Clone, Copy)]
struct Failures {
    in_a_row: u32,
    last_at: u64,
}

impl FailedSignIns {
    /// Nothing counted yet.
    pub fn new() -> Self {
        FailedSignIns {
            failures: HashMap::new(),
            next_cleanup_at: 0,
        }
    }

    /// How many seconds `email` from `address` has to wait before trying
    /// again at `now`, or `None` when it may try now.
    pub fn wait(&self, email: &str, address: IpAddr, now: u64) -> Option<u64> {
        let lock_end = counted(email, address)
            .iter()
            .filter_map(|key| self.failures.get(key))
            .filter_map(|failures| failures.locked_until(now))
            .max()?;
        Some(lock_end - now)
    }

    /// Counts a failed sign-in of `email` from `address` at `now`.
    pub fn record_failure(&mut self, email: &str, address: IpAddr, now: u64) {
        self.forget_stale(now);
        for key in counted(email, address) {
            let failures = self.failures.entry(key).or_insert(Failures {
                in_a_row: 0,
                last_at: now,
            });
            let in_a_row = if failures.is_stale(now) {
                1
            } else {
                failures.in_a_row.saturating_add(1)
            };
            *failures = Failures {
                in_a_row,
                last_at: now,
            };
        }
    }

    /// Forgets the failures of `email` after it signed in.
    ///
    /// The address keeps its count: otherwise a guesser could sign in to an
    /// account of their own between guesses and never be locked.
    pub fn record_success(&mut self, email: &str) {
        self.failures
            .remove(&Counted::Email(normalize_email(email)));
    }

    /// Drops counts nobody has added to for a day, so emails and addresses
    /// tried once do not pile up in memory.
    fn forget_stale(&mut self, now: u64) {
        if now < self.next_cleanup_at {
            return;
        }
        self.failures.retain(|_, failures| !failures.is_stale(now));
        self.next_cleanup_at = now + FORGET_AFTER_SECONDS;
    }
}

impl Failures {
    /// When the lock these failures earn ends, if it ends after `now`.
    fn locked_until(&self, now: u64) -> Option<u64> {
        if self.in_a_row < FREE_FAILURES || self.is_stale(now) {
            return None;
        }
        let lock = 2u64
            .saturating_pow(self.in_a_row - FREE_FAILURES)
            .saturating_mul(FIRST_LOCK_SECONDS)
            .min(MAX_LOCK_SECONDS);
        let lock_end = self.last_at + lock;
        (lock_end > now).then_some(lock_end)
    }

    fn is_stale(&self, now: u64) -> bool {
        now >= self.last_at + FORGET_AFTER_SECONDS
    }
}

/// The email and the address that a sign-in is counted against.
fn counted(email: &str, address: IpAddr) -> [Counted; 2] {
    let address = match address.to_canonical() {
        IpAddr::V4(address) => IpAddr::V4(address),
        IpAddr::V6(address) => {
            let network = address.to_bits() & !u128::from(u64::MAX);
            IpAddr::V6(Ipv6Addr::from_bits(network))
        }
    };
    [
        Counted::Email(normalize_email(email)),
        Counted::Address(address),
    ]
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use super::*;

    const HOME: IpAddr = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));
    const ELSEWHERE: IpAddr = IpAddr::V4(Ipv4Addr::new(198, 51, 100, 7));

    fn fail(failed: &mut FailedSignIns, email: &str, address: IpAddr, times: u32, now: u64) {
        for _ in 0..times {
            failed.record_failure(email, address, now);
        }
    }

    #[test]
    fn a_few_failures_are_free_and_then_the_wait_doubles() {
        let mut failed = FailedSignIns::new();

        fail(&mut failed, "ana@example.org", HOME, FREE_FAILURES - 1, 0);
        assert_eq!(failed.wait("ana@example.org", HOME, 0), None);

        fail(&mut failed, "ana@example.org", HOME, 1, 0);
        assert_eq!(failed.wait("ana@example.org", HOME, 0), Some(60));
        assert_eq!(failed.wait("ana@example.org", HOME, 60), None);

        fail(&mut failed, "ana@example.org", HOME, 1, 60);
        assert_eq!(failed.wait("ana@example.org", HOME, 60), Some(120));
    }

    #[test]
    fn the_wait_stops_growing_at_the_longest_lock() {
        let mut failed = FailedSignIns::new();

        fail(&mut failed, "ana@example.org", HOME, 100, 0);

        assert_eq!(
            failed.wait("ana@example.org", HOME, 0),
            Some(MAX_LOCK_SECONDS)
        );
    }

    #[test]
    fn an_email_stays_locked_from_another_address() {
        let mut failed = FailedSignIns::new();

        fail(&mut failed, "ana@example.org", HOME, FREE_FAILURES, 0);

        assert!(failed.wait("Ana@Example.org", ELSEWHERE, 1).is_some());
    }

    #[test]
    fn an_address_stays_locked_for_another_email() {
        let mut failed = FailedSignIns::new();

        fail(&mut failed, "ana@example.org", HOME, FREE_FAILURES, 0);

        assert!(failed.wait("bob@example.org", HOME, 1).is_some());
        assert_eq!(failed.wait("bob@example.org", ELSEWHERE, 1), None);
    }

    #[test]
    fn addresses_in_one_ipv6_network_count_together() {
        let mut failed = FailedSignIns::new();
        let first: IpAddr = "2001:db8:1:2::1".parse().unwrap();
        let second: IpAddr = "2001:db8:1:2:ffff::9".parse().unwrap();
        let other_network: IpAddr = "2001:db8:1:3::1".parse().unwrap();

        for number in 0..FREE_FAILURES {
            failed.record_failure(&format!("user{number}@example.org"), first, 0);
        }

        assert!(failed.wait("someone@example.org", second, 1).is_some());
        assert_eq!(failed.wait("someone@example.org", other_network, 1), None);
    }

    #[test]
    fn signing_in_clears_the_email_but_not_the_address() {
        let mut failed = FailedSignIns::new();
        fail(&mut failed, "ana@example.org", HOME, FREE_FAILURES, 0);

        failed.record_success("ana@example.org");

        assert_eq!(failed.wait("ana@example.org", ELSEWHERE, 1), None);
        assert!(failed.wait("ana@example.org", HOME, 1).is_some());
    }

    #[test]
    fn failures_are_forgotten_after_a_quiet_day() {
        let mut failed = FailedSignIns::new();
        fail(&mut failed, "ana@example.org", HOME, FREE_FAILURES + 3, 0);

        fail(
            &mut failed,
            "ana@example.org",
            HOME,
            1,
            FORGET_AFTER_SECONDS,
        );

        assert_eq!(
            failed.wait("ana@example.org", HOME, FORGET_AFTER_SECONDS),
            None
        );
    }
}
