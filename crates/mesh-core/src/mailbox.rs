//! Mailboxes: small opaque messages left on nodes for one person, without a
//! server learning who writes to whom.
//!
//! A person's mailbox address is the hash of their mailbox public key and
//! the week ([`personal_address`]), so it changes every week and nodes can't
//! link a mailbox over time. It sits on about ten nodes chosen the way pointer
//! nodes are. A node keeps an append-only list of messages per address for
//! [`MESSAGE_LEASE_SECONDS`] and hands them to anyone who asks; only the
//! recipient can open them.
//!
//! Anyone who knows the public key can write to the personal mailbox, so a
//! message there carries a proof of work: a nonce that makes
//! `BLAKE3(address ‖ BLAKE3(message) ‖ week ‖ nonce)` start with
//! [`WORK_BITS`] zero bits. Finding it takes about a second on a phone; the
//! node checks it with one hash. Contacts write to a mailbox of their own at
//! the hash of a secret only the two share ([`contact_address`]), which
//! strangers can't compute. The two kinds of mailbox live under different
//! routes, so a stranger writing to someone's personal address under the
//! contact route reaches a box nobody reads.
//!
//! A node can't tell a real contact address from a made-up one, though, so
//! anyone could fill its mailbox space with junk at random contact
//! addresses. A contact message therefore carries a far smaller proof of
//! work, [`CONTACT_WORK_BITS`], a few milliseconds for one message. A node
//! also takes at most [`MAX_MESSAGES_PER_SENDER`] messages a week into one
//! contact mailbox from each sending address, counting an IPv6 /64 as one
//! address.
//!
//! People find each other's mailbox key by email at a coordinator, which
//! signs what it answers ([`MailboxKey`]).
//!
//! A node answers:
//!
//! - `PUT /mailboxes/{address}?week=W&nonce=N` with the message as the body
//!   appends it, when the work holds and `W` is this week or last.
//! - `PUT /contact-mailboxes/{address}?week=W&nonce=N` the same, with the
//!   contact work, and `429 Too Many Requests` once the sending address has
//!   left its share of messages there this week.
//! - `GET /mailboxes/{address}` and `GET /contact-mailboxes/{address}`
//!   return the messages as [`MailboxContents`].
//!
//! Appending a message the mailbox already holds changes nothing, so a
//! sender can retry. A full mailbox answers `507 Insufficient Storage`.
//!
//! A coordinator answers:
//!
//! - `PUT /mailbox-keys` with a [`PublishMailboxKey`] files a signed-in
//!   account's mailbox key under its email.
//! - `GET /mailbox-keys?email=E` returns a [`MailboxKey`] signed with the
//!   coordinator's key. An email without a key gets a made-up one that looks
//!   like any other, so the answer does not tell which emails have accounts.

use serde::{Deserialize, Serialize};

use crate::signed::SignedBody;
use crate::tokens::HexBytes;

/// The route of a personal mailbox, in the router syntax of `axum`.
pub const MAILBOX_ROUTE: &str = "/mailboxes/{address}";
/// The route of a contact mailbox, in the router syntax of `axum`.
pub const CONTACT_MAILBOX_ROUTE: &str = "/contact-mailboxes/{address}";
/// The route a coordinator files and hands out mailbox keys at.
pub const MAILBOX_KEYS_ROUTE: &str = "/mailbox-keys";

/// How long a mailbox week is.
pub const WEEK_SECONDS: u64 = 7 * 24 * 60 * 60;

/// How long a node keeps a message: the week its address is written in, and
/// another for the recipient to read it.
pub const MESSAGE_LEASE_SECONDS: u64 = 2 * WEEK_SECONDS;

/// The zero bits a message to a personal mailbox has to buy. 2^20 hashes are
/// about a second on a phone and nothing for one message, but a spammer
/// pays it for every message.
pub const WORK_BITS: u32 = 20;

/// The zero bits a message to a contact mailbox has to buy: about 4,000
/// hashes, a few milliseconds on a phone. Filling a node's 1 GiB of mailbox
/// space with 1 KiB messages then takes about four billion hashes, again for
/// every node and every two weeks.
pub const CONTACT_WORK_BITS: u32 = 12;

/// The most messages a node takes into one contact mailbox from one sending
/// address in a week. A real contact sends a few; a flooder gets this many
/// per mailbox for each address it controls.
pub const MAX_MESSAGES_PER_SENDER: u32 = 16;

/// The longest message. A share is a key and a pointer address, far below
/// this; the limit keeps a mailbox from holding files.
pub const MAX_MESSAGE_BYTES: usize = 16 * 1024;

/// The most messages one mailbox holds. A mailbox lives two weeks, so this
/// is plenty for shares, and caps what one address can take on a node.
pub const MAX_MESSAGES_PER_MAILBOX: usize = 256;

/// Which kind of mailbox an address names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MailboxKind {
    /// At the hash of a public key: anyone may write, with proof of work.
    Personal,
    /// At the hash of a shared secret: only contacts can find it.
    Contact,
}

impl MailboxKind {
    /// The path of the mailbox `address` of this kind on a node.
    pub fn path(self, address: &[u8; 32]) -> String {
        let address = hex::encode(address);
        match self {
            MailboxKind::Personal => format!("/mailboxes/{address}"),
            MailboxKind::Contact => format!("/contact-mailboxes/{address}"),
        }
    }

    /// The zero bits a message to a mailbox of this kind has to buy.
    pub fn work_bits(self) -> u32 {
        match self {
            MailboxKind::Personal => WORK_BITS,
            MailboxKind::Contact => CONTACT_WORK_BITS,
        }
    }
}

/// The mailbox week `unix_seconds` falls in.
pub fn week_of(unix_seconds: u64) -> u64 {
    unix_seconds / WEEK_SECONDS
}

/// The personal mailbox address of the person with `public_key` in `week`.
pub fn personal_address(public_key: &[u8; 32], week: u64) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key("neoworks-mesh personal mailbox v1");
    hasher.update(public_key);
    hasher.update(&week.to_be_bytes());
    *hasher.finalize().as_bytes()
}

/// The address `recipient` reads messages from a contact at in `week`, where
/// `shared_secret` is what only the two of them can compute. Each direction
/// has its own address, so each reads only what is written to them.
pub fn contact_address(shared_secret: &[u8; 32], recipient: &[u8; 32], week: u64) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key("neoworks-mesh contact mailbox v1");
    hasher.update(shared_secret);
    hasher.update(recipient);
    hasher.update(&week.to_be_bytes());
    *hasher.finalize().as_bytes()
}

/// Whether `nonce` is enough work for `message` to the mailbox of `kind` at
/// `address` in `week`.
pub fn work_holds(
    kind: MailboxKind,
    address: &[u8; 32],
    message: &[u8],
    week: u64,
    nonce: u64,
) -> bool {
    let message_hash = blake3::hash(message);
    let hash = work_hash(address, message_hash.as_bytes(), week, nonce);
    leading_zero_bits(&hash) >= kind.work_bits()
}

/// The first nonce that is enough work for `message` to the mailbox of
/// `kind` at `address` in `week`. Takes about 2^[`MailboxKind::work_bits`]
/// hashes.
pub fn find_work(kind: MailboxKind, address: &[u8; 32], message: &[u8], week: u64) -> u64 {
    let message_hash = blake3::hash(message);
    // A nonce is found within a few million tries with all but vanishing
    // odds, far below u64::MAX.
    (0..u64::MAX)
        .find(|&nonce| {
            let hash = work_hash(address, message_hash.as_bytes(), week, nonce);
            leading_zero_bits(&hash) >= kind.work_bits()
        })
        .unwrap_or(u64::MAX)
}

fn work_hash(address: &[u8; 32], message_hash: &[u8; 32], week: u64, nonce: u64) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(address);
    hasher.update(message_hash);
    hasher.update(&week.to_be_bytes());
    hasher.update(&nonce.to_be_bytes());
    *hasher.finalize().as_bytes()
}

fn leading_zero_bits(hash: &[u8; 32]) -> u32 {
    let mut bits = 0;
    for byte in hash {
        bits += byte.leading_zeros();
        if *byte != 0 {
            break;
        }
    }
    bits
}

/// The query of a message to a mailbox of either kind: the week its work
/// covers and the nonce.
#[derive(Debug, Serialize, Deserialize)]
pub struct WorkQuery {
    pub week: u64,
    pub nonce: u64,
}

/// The messages a mailbox holds, oldest first.
#[derive(Debug, Serialize, Deserialize)]
pub struct MailboxContents {
    pub messages: Vec<HexBytes>,
}

/// A signed-in account filing its mailbox key.
#[derive(Debug, Serialize, Deserialize)]
pub struct PublishMailboxKey {
    pub email: String,
    /// The auth key the account signs in with (`crate::sign_in`).
    #[serde(with = "hex")]
    pub auth_key: [u8; 32],
    /// The account's X25519 mailbox public key.
    #[serde(with = "hex")]
    pub public_key: [u8; 32],
}

/// The query that looks up a mailbox key.
#[derive(Debug, Serialize, Deserialize)]
pub struct MailboxKeyQuery {
    pub email: String,
}

/// The mailbox key filed under an email, as a coordinator vouches for it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MailboxKey {
    /// The email as accounts normalize it.
    pub email: String,
    /// The X25519 public key messages to this person are sealed to.
    #[serde(with = "hex")]
    pub public_key: [u8; 32],
}

impl SignedBody for MailboxKey {
    const CONTEXT: &'static str = "neoworks-mesh mailbox key v1";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addresses_change_every_week_and_differ_by_kind() {
        let key = [7; 32];

        assert_ne!(personal_address(&key, 1), personal_address(&key, 2));
        assert_ne!(
            personal_address(&key, 1),
            contact_address(&key, &[0; 32], 1)
        );
        assert_ne!(
            contact_address(&key, &[1; 32], 1),
            contact_address(&key, &[2; 32], 1)
        );
    }

    #[test]
    fn found_work_holds_only_for_its_message_address_and_week() {
        let personal = MailboxKind::Personal;
        let address = personal_address(&[7; 32], 3);
        let nonce = find_work(personal, &address, b"a share", 3);

        assert!(work_holds(personal, &address, b"a share", 3, nonce));
        assert!(!work_holds(personal, &address, b"another share", 3, nonce));
        assert!(!work_holds(personal, &address, b"a share", 4, nonce));
        assert!(!work_holds(personal, &[0; 32], b"a share", 3, nonce));
    }

    #[test]
    fn contact_work_is_not_enough_for_a_personal_mailbox() {
        let address = contact_address(&[7; 32], &[1; 32], 3);
        // The first nonce with 12 zero bits but fewer than 20: about one in
        // 256 nonces that hold for contacts holds for personal mailboxes too.
        let nonce = (0..u64::MAX)
            .find(|&nonce| {
                work_holds(MailboxKind::Contact, &address, b"a note", 3, nonce)
                    && !work_holds(MailboxKind::Personal, &address, b"a note", 3, nonce)
            })
            .unwrap();

        assert!(work_holds(
            MailboxKind::Contact,
            &address,
            b"a note",
            3,
            nonce
        ));
        assert!(!work_holds(
            MailboxKind::Personal,
            &address,
            b"a note",
            3,
            nonce
        ));
    }

    #[test]
    fn zero_bits_are_counted_across_bytes() {
        let mut hash = [0xff; 32];
        assert_eq!(leading_zero_bits(&hash), 0);
        hash[0] = 0;
        hash[1] = 0;
        hash[2] = 0x0f;
        assert_eq!(leading_zero_bits(&hash), 20);
    }
}
