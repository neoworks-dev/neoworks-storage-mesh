//! Sending a person small sealed messages through mailboxes on nodes, and
//! reading one's own (`mesh_core::mailbox` describes mailboxes).
//!
//! A message is sealed to the recipient's X25519 mailbox key: the sender
//! makes a fresh key pair, and the shared secret of it and the recipient's
//! key seals the message with XChaCha20-Poly1305. Only the recipient can open
//! it, and nothing in it says who sent it. The sealed message goes to every
//! node of the mailbox, chosen from its address the way pointer nodes are, so
//! it survives some of them going away.
//!
//! A stranger writes to the recipient's personal mailbox and pays proof of
//! work for it, about a second. Contacts write to a mailbox only the two of
//! them can compute, from the shared secret of their own two mailbox keys,
//! and pay a few milliseconds of work; a message there is known to come from
//! that contact.
//!
//! Mailbox keys are found by email at a coordinator, which signs its answer.

use std::collections::HashSet;

use futures::future::join_all;
use mesh_core::mailbox::{
    MAILBOX_KEYS_ROUTE, MailboxContents, MailboxKey, MailboxKind, PublishMailboxKey,
    contact_address, find_work, personal_address, week_of,
};
use mesh_core::node_list::{CoordinatorList, TrustError};
use mesh_core::sign_in::normalize_email;
use mesh_core::signed::Signed;
use reqwest::header::CONTENT_TYPE;
use reqwest::{StatusCode, Url};
use x25519_dalek::{X25519_BASEPOINT_BYTES, x25519};

use crate::encryption::{MessageKey, open_message, seal_message};
use crate::node_client::NodeClient;
use crate::node_list::NodeList;
use crate::pointers::pointer_nodes;

/// The sender's one-time public key, which a sealed message starts with.
const EPHEMERAL_KEY_BYTES: usize = 32;

/// A message that could not be sent.
#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error("no node of the mailbox took the message")]
    NowhereStored,
    #[error("finding the proof of work failed: {0}")]
    Work(#[from] tokio::task::JoinError),
}

/// A mailbox key that could not be filed or found.
#[derive(Debug, thiserror::Error)]
pub enum MailboxKeyError {
    #[error("the coordinator could not be reached: {0}")]
    Unreachable(#[from] reqwest::Error),
    #[error("wrong email or password")]
    WrongPassword,
    #[error("the coordinator answered {status}: {message}")]
    Refused { status: StatusCode, message: String },
    #[error("the coordinator answered in a form this client does not understand")]
    UnexpectedAnswer,
    #[error("the mailbox key is not to be trusted: {0}")]
    Untrusted(#[from] TrustError),
    #[error("the coordinator answered for another email")]
    OtherEmail,
    #[error("{0} is not a URL")]
    BadUrl(String),
}

/// A message read from one's mailboxes.
#[derive(Debug, PartialEq, Eq)]
pub struct Received {
    /// The contact whose mailbox it came through, by mailbox key, or `None`
    /// for the personal mailbox, where anyone may write.
    pub from_contact: Option<[u8; 32]>,
    pub message: Vec<u8>,
}

/// The mailbox public key of the secret `secret`.
pub fn mailbox_public_key(secret: &[u8; 32]) -> [u8; 32] {
    x25519(*secret, X25519_BASEPOINT_BYTES)
}

/// `message` sealed so that only the holder of the secret of `recipient` can
/// open it.
pub fn seal_to(recipient: &[u8; 32], message: &[u8]) -> Vec<u8> {
    let mut ephemeral_secret = [0u8; 32];
    getrandom::fill(&mut ephemeral_secret).expect("the operating system's random source works");
    let ephemeral_public = mailbox_public_key(&ephemeral_secret);
    let shared = x25519(ephemeral_secret, *recipient);
    let key = seal_key(&shared, &ephemeral_public, recipient);
    let mut sealed = ephemeral_public.to_vec();
    sealed.extend_from_slice(&seal_message(&key, message));
    sealed
}

/// The message in `sealed`, when it was sealed to the public key of
/// `secret`; `None` for anything else, such as a stranger's junk.
pub fn open_sealed(secret: &[u8; 32], sealed: &[u8]) -> Option<Vec<u8>> {
    if sealed.len() < EPHEMERAL_KEY_BYTES {
        return None;
    }
    let (ephemeral_public, ciphertext) = sealed.split_at(EPHEMERAL_KEY_BYTES);
    let ephemeral_public: [u8; 32] = ephemeral_public.try_into().ok()?;
    let shared = x25519(*secret, ephemeral_public);
    let key = seal_key(&shared, &ephemeral_public, &mailbox_public_key(secret));
    open_message(&key, ciphertext).ok()
}

/// The key one sealed message is sealed under. It covers both public keys as
/// well as the shared secret, so a message can't be passed off as sealed to
/// someone else.
fn seal_key(shared: &[u8; 32], ephemeral_public: &[u8; 32], recipient: &[u8; 32]) -> MessageKey {
    let mut hasher = blake3::Hasher::new_derive_key("neoworks-mesh mailbox seal v1");
    hasher.update(shared);
    hasher.update(ephemeral_public);
    hasher.update(recipient);
    MessageKey::from_bytes(*hasher.finalize().as_bytes())
}

/// The secret a contact pair shares: what each computes from their own
/// mailbox secret and the other's public key.
fn contact_secret(own_secret: &[u8; 32], contact: &[u8; 32]) -> [u8; 32] {
    x25519(*own_secret, *contact)
}

/// Sends `message` to the personal mailbox of `recipient` on nodes from
/// `list`, with the proof of work strangers pay, and returns on how many
/// nodes it is.
pub async fn send(
    client: &NodeClient,
    list: &NodeList,
    recipient: &[u8; 32],
    message: &[u8],
    now: u64,
) -> Result<usize, SendError> {
    let week = week_of(now);
    let address = personal_address(recipient, week);
    let sealed = seal_to(recipient, message);
    deliver_with_work(client, list, MailboxKind::Personal, address, week, sealed).await
}

/// Sends `message` to `contact` through the mailbox only the two of them can
/// compute, from `own_secret`, and returns on how many nodes it is.
pub async fn send_to_contact(
    client: &NodeClient,
    list: &NodeList,
    own_secret: &[u8; 32],
    contact: &[u8; 32],
    message: &[u8],
    now: u64,
) -> Result<usize, SendError> {
    let week = week_of(now);
    let shared = contact_secret(own_secret, contact);
    let address = contact_address(&shared, contact, week);
    let sealed = seal_to(contact, message);
    deliver_with_work(client, list, MailboxKind::Contact, address, week, sealed).await
}

/// Finds the work `sealed` needs for the mailbox of `kind` at `address` in
/// `week`, then delivers it there.
async fn deliver_with_work(
    client: &NodeClient,
    list: &NodeList,
    kind: MailboxKind,
    address: [u8; 32],
    week: u64,
    sealed: Vec<u8>,
) -> Result<usize, SendError> {
    let for_work = sealed.clone();
    // Up to a second of hashing, which would hold up every other task on
    // this thread.
    let nonce =
        tokio::task::spawn_blocking(move || find_work(kind, &address, &for_work, week)).await?;
    let query = format!("?week={week}&nonce={nonce}");
    deliver(client, list, kind, &address, &query, &sealed).await
}

/// Appends `sealed` to the mailbox `address` on each of its nodes.
async fn deliver(
    client: &NodeClient,
    list: &NodeList,
    kind: MailboxKind,
    address: &[u8; 32],
    query: &str,
    sealed: &[u8],
) -> Result<usize, SendError> {
    let path = kind.path(address);
    let puts = pointer_nodes(address, list).into_iter().map(|node| {
        let url = format!("{}{path}{query}", node.url.trim_end_matches('/'));
        async move {
            let response = client.http().put(url).body(sealed.to_vec()).send().await;
            match response {
                Ok(response) if response.status().is_success() => true,
                Ok(response) => {
                    eprintln!("mailbox on {}: {}", node.url, response.status());
                    false
                }
                Err(error) => {
                    eprintln!("mailbox on {}: {error}", node.url);
                    false
                }
            }
        }
    });
    let stored = join_all(puts)
        .await
        .into_iter()
        .filter(|&took| took)
        .count();
    if stored == 0 {
        return Err(SendError::NowhereStored);
    }
    Ok(stored)
}

/// Every message for the holder of `own_secret` in its personal mailbox and
/// in its mailboxes with each of `contacts`, this week and last, from nodes
/// in `list`. Messages that don't open are left out.
pub async fn receive(
    client: &NodeClient,
    list: &NodeList,
    own_secret: &[u8; 32],
    contacts: &[[u8; 32]],
    now: u64,
) -> Vec<Received> {
    let own_public = mailbox_public_key(own_secret);
    let this_week = week_of(now);
    let mut received = Vec::new();
    // Messages stay two weeks, so last week's mailbox may still hold some.
    for week in [this_week.saturating_sub(1), this_week] {
        let address = personal_address(&own_public, week);
        for sealed in read(client, list, MailboxKind::Personal, &address).await {
            if let Some(message) = open_sealed(own_secret, &sealed) {
                received.push(Received {
                    from_contact: None,
                    message,
                });
            }
        }
        for contact in contacts {
            let shared = contact_secret(own_secret, contact);
            let address = contact_address(&shared, &own_public, week);
            for sealed in read(client, list, MailboxKind::Contact, &address).await {
                if let Some(message) = open_sealed(own_secret, &sealed) {
                    received.push(Received {
                        from_contact: Some(*contact),
                        message,
                    });
                }
            }
        }
    }
    received
}

/// The messages in the mailbox `address`, gathered from all of its nodes,
/// each once.
async fn read(
    client: &NodeClient,
    list: &NodeList,
    kind: MailboxKind,
    address: &[u8; 32],
) -> Vec<Vec<u8>> {
    let path = kind.path(address);
    let gets = pointer_nodes(address, list).into_iter().map(|node| {
        let url = format!("{}{path}", node.url.trim_end_matches('/'));
        async move {
            let response = client.http().get(url).send().await.ok()?;
            let body = response.bytes().await.ok()?;
            serde_json::from_slice::<MailboxContents>(&body).ok()
        }
    });
    let mut seen = HashSet::new();
    let mut messages = Vec::new();
    for contents in join_all(gets).await.into_iter().flatten() {
        for message in contents.messages {
            if seen.insert(message.0.clone()) {
                messages.push(message.0);
            }
        }
    }
    messages
}

/// Files `public_key` as the mailbox key of the account `email`, proven with
/// its `auth_key`, at `coordinator`.
pub async fn publish_key(
    client: &NodeClient,
    coordinator: &str,
    email: &str,
    auth_key: [u8; 32],
    public_key: [u8; 32],
) -> Result<(), MailboxKeyError> {
    let request = PublishMailboxKey {
        email: email.to_string(),
        auth_key,
        public_key,
    };
    // A request is plain strings and bytes, which always serialize.
    let body = serde_json::to_vec(&request).expect("a key request serializes");
    let url = format!("{}{MAILBOX_KEYS_ROUTE}", coordinator.trim_end_matches('/'));
    let response = client
        .http()
        .put(url)
        .header(CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await?;
    match response.status() {
        status if status.is_success() => Ok(()),
        StatusCode::UNAUTHORIZED => Err(MailboxKeyError::WrongPassword),
        status => Err(MailboxKeyError::Refused {
            status,
            message: response.text().await.unwrap_or_default(),
        }),
    }
}

/// The mailbox key of `email` at `coordinator`, once a coordinator on
/// `coordinators` is found to have signed it for that email.
pub async fn look_up_key(
    client: &NodeClient,
    coordinator: &str,
    coordinators: &CoordinatorList,
    email: &str,
) -> Result<[u8; 32], MailboxKeyError> {
    let base = format!("{}{MAILBOX_KEYS_ROUTE}", coordinator.trim_end_matches('/'));
    // Emails may hold characters such as `+` that a query has to escape.
    let url = Url::parse_with_params(&base, [("email", email)])
        .map_err(|_| MailboxKeyError::BadUrl(base.clone()))?;
    let response = client.http().get(url).send().await?;
    if !response.status().is_success() {
        return Err(MailboxKeyError::Refused {
            status: response.status(),
            message: response.text().await.unwrap_or_default(),
        });
    }
    let body = response.bytes().await?;
    let key: Signed<MailboxKey> =
        serde_json::from_slice(&body).map_err(|_| MailboxKeyError::UnexpectedAnswer)?;
    coordinators.check_signed(&key)?;
    // A signed answer for another email would hand out someone else's key.
    if key.body.email != normalize_email(email) {
        return Err(MailboxKeyError::OtherEmail);
    }
    Ok(key.body.public_key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_recipient_opens_a_sealed_message() {
        let recipient = [3; 32];
        let stranger = [4; 32];

        let sealed = seal_to(&mailbox_public_key(&recipient), b"the collection key");

        assert_eq!(
            open_sealed(&recipient, &sealed),
            Some(b"the collection key".to_vec())
        );
        assert_eq!(open_sealed(&stranger, &sealed), None);
        assert_eq!(open_sealed(&recipient, &sealed[..20]), None);
    }

    #[test]
    fn two_contacts_compute_the_same_secret() {
        let ana = [5; 32];
        let bob = [6; 32];

        assert_eq!(
            contact_secret(&ana, &mailbox_public_key(&bob)),
            contact_secret(&bob, &mailbox_public_key(&ana))
        );
    }
}
