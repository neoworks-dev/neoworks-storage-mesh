//! Telling the account's other devices that a head moved, so they fetch at
//! once instead of polling every pointer.
//!
//! A device that moves a pointer leaves a short notice: the pointer's owner
//! key and its new version. All devices of an account share one mailbox
//! key, so the notice goes to the contact mailbox the account shares with
//! itself ([`mailboxes`](crate::mailboxes)). Only holders of the account's
//! secret can compute that address, so writing to it needs no proof of work.
//!
//! A notice is only a hint. A device that reads one still reads the pointer
//! and checks it as always, so a lost, repeated or made-up notice costs at
//! most a read. A mailbox holds a limited number of messages a week; once
//! it is full, devices fall back to polling until the week turns.

use serde::{Deserialize, Serialize};

use crate::account::Account;
use crate::mailboxes::{SendError, mailbox_public_key, receive, send_to_contact};
use crate::node_client::NodeClient;
use crate::node_list::NodeList;

/// A pointer that moved, and the version it moved to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeNotice {
    /// The public key the pointer is filed under.
    #[serde(with = "hex")]
    pub head_owner: [u8; 32],
    pub version: u64,
}

/// Leaves `notice` for the other devices of `account`, and returns on how
/// many nodes it is.
pub async fn send_notice(
    client: &NodeClient,
    list: &NodeList,
    account: &Account,
    notice: ChangeNotice,
    now: u64,
) -> Result<usize, SendError> {
    let secret = account.mailbox_secret();
    // A notice is a key and a number, which always serialize.
    let message = serde_json::to_vec(&notice).expect("a change notice serializes");
    let own_public = mailbox_public_key(&secret);
    send_to_contact(client, list, &secret, &own_public, &message, now).await
}

/// The notices the devices of `account` left this week and last, the
/// highest version once per pointer, sorted by pointer.
pub async fn read_notices(
    client: &NodeClient,
    list: &NodeList,
    account: &Account,
    now: u64,
) -> Vec<ChangeNotice> {
    let secret = account.mailbox_secret();
    let own_public = mailbox_public_key(&secret);
    let received = receive(client, list, &secret, &[own_public], now).await;
    let notices = received
        .into_iter()
        // The account's personal mailbox holds other people's messages.
        .filter(|message| message.from_contact == Some(own_public))
        .filter_map(|message| serde_json::from_slice(&message.message).ok());
    newest_per_pointer(notices)
}

/// The highest version of each pointer among `notices`, sorted by pointer.
fn newest_per_pointer(notices: impl Iterator<Item = ChangeNotice>) -> Vec<ChangeNotice> {
    let mut newest: Vec<ChangeNotice> = Vec::new();
    for notice in notices {
        match newest
            .iter_mut()
            .find(|kept| kept.head_owner == notice.head_owner)
        {
            Some(kept) => kept.version = kept.version.max(notice.version),
            None => newest.push(notice),
        }
    }
    newest.sort_by_key(|notice| notice.head_owner);
    newest
}

#[cfg(test)]
mod tests {
    use super::*;

    fn notice(owner: u8, version: u64) -> ChangeNotice {
        ChangeNotice {
            head_owner: [owner; 32],
            version,
        }
    }

    #[test]
    fn only_the_highest_version_of_each_pointer_is_kept() {
        let notices = [notice(2, 4), notice(1, 7), notice(2, 9), notice(2, 5)];

        let newest = newest_per_pointer(notices.into_iter());

        assert_eq!(newest, [notice(1, 7), notice(2, 9)]);
    }
}
