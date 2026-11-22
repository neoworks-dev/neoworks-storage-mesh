//! The signed node list: what nodes say about themselves, and the snapshot of
//! all of them a coordinator publishes.
//!
//! A node signs a [`NodeRecord`] with its own key, so nobody else can move it
//! to another address or claim to be it. A coordinator collects the records,
//! measures each node's uptime itself and signs a [`NodeSnapshot`] of all of
//! them. Clients download the snapshot whole, so looking up a node tells
//! nobody which node a client is interested in.
//!
//! Which coordinators to trust is a [`CoordinatorList`] the client starts
//! with. It is versioned and each new version is signed by more than half of
//! the coordinators of the last one, so coordinators can be added without a
//! protocol change and no single key can take the list over.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::NodeId;
use crate::signed::{MultiSigned, SignatureError, Signed, SignedBody};

/// What a node says about itself.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRecord {
    /// The hash of the key the record is signed with.
    pub node_id: NodeId,
    /// Where the node is reached, e.g. `http://203.0.113.7:7001`.
    pub url: String,
    /// Who runs the node. Nodes of one operator can all fail at once.
    pub operator: String,
    /// The network the node is reached through, such as an AS number.
    pub network: String,
    /// Where the node says it is, such as `de` or `eu-west`.
    pub region: String,
    /// The bytes the node offers to store.
    pub capacity_bytes: u64,
    /// When the node signed the record, in Unix seconds. A newer record
    /// replaces an older one, so a record the node replaced cannot be
    /// replayed to move it back to an old address.
    pub signed_at: u64,
}

impl SignedBody for NodeRecord {
    const CONTEXT: &'static str = "neoworks-mesh node record v1";
}

/// Why a node record is not genuine.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum NodeRecordError {
    #[error("{0}")]
    Signature(#[from] SignatureError),
    #[error("the record names node {claimed}, but was signed by node {signer}")]
    WrongSigner { claimed: NodeId, signer: NodeId },
}

impl Signed<NodeRecord> {
    /// Checks that the node the record names signed it.
    pub fn verify_node(&self) -> Result<(), NodeRecordError> {
        self.verify()?;
        let signer = NodeId::of(&self.signer);
        if signer != self.body.node_id {
            return Err(NodeRecordError::WrongSigner {
                claimed: self.body.node_id,
                signer,
            });
        }
        Ok(())
    }
}

/// One node in a snapshot: its own record and what the coordinator measured.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListedNode {
    pub record: Signed<NodeRecord>,
    /// The share of the coordinator's checks the node answered over the last
    /// day, in thousandths. An integer, so the snapshot's JSON is the same
    /// on every machine.
    pub uptime_permille: u16,
    /// The share of the coordinator's audits the node passed over the last
    /// day, in thousandths, or `None` when the coordinator could not audit
    /// it. The coordinator keeps a piece of its own on every node and asks
    /// for proofs of it, so a node that answers checks but threw its pieces
    /// away shows here.
    pub audit_pass_permille: Option<u16>,
    /// The median of the node's recent speed tests, in bits per second, or
    /// `None` before its first one.
    pub bandwidth_bits_per_second: Option<u64>,
    /// The bytes the node said it holds when last asked, or `None` when it
    /// never said. Only the node knows this, so it is a claim; prices
    /// ([`crate::prices`]) count it no higher than the node's capacity.
    pub used_bytes: Option<u64>,
    /// Whether the node is run by the coordinator itself, and so takes
    /// allowance tokens ([`crate::tokens::TokenKind::Allowance`]).
    pub takes_allowance: bool,
    /// The network the coordinator found the node's address in, such as
    /// `AS3320`, or `None` when it could not tell. Unlike the record's
    /// `network`, the node can't choose it.
    pub measured_network: Option<String>,
    /// The country the coordinator located the node's address in, such as
    /// `DE`, once round-trip times did not rule it out; `None` when unknown.
    pub measured_country: Option<String>,
    /// The same for every node whose earnings go to one account, and says
    /// nothing else about the account; `None` while the node names no
    /// operator. Unlike the record's `operator`, the node can't choose it.
    pub operator_id: Option<String>,
    /// Whether the node failed its last three checks within ten minutes.
    /// Clients neither read from it nor place pieces on it, and the
    /// coordinator repairs what it held.
    pub down: bool,
}

/// Every node a coordinator knows, at one moment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSnapshot {
    /// When the snapshot was made, in Unix seconds. Clients keep the newest
    /// they have seen, so an old snapshot cannot be replayed to them.
    pub published_at: u64,
    pub nodes: Vec<ListedNode>,
    /// The chance that a node is lost for good on any one day, in millionths,
    /// measured over the nodes the coordinator watched recently; `None` while
    /// it has watched too little to say. Uptime is not this: a node that is
    /// offline for an hour still has its pieces.
    pub daily_loss_per_million: Option<u32>,
}

impl SignedBody for NodeSnapshot {
    const CONTEXT: &'static str = "neoworks-mesh node snapshot v1";
}

/// The public key of a coordinator.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoordinatorKey(#[serde(with = "hex")] pub [u8; 32]);

/// The coordinators whose snapshots a client accepts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoordinatorList {
    /// Goes up with every change to the list.
    pub version: u64,
    pub keys: Vec<CoordinatorKey>,
}

impl SignedBody for CoordinatorList {
    const CONTEXT: &'static str = "neoworks-mesh coordinator list v1";
}

/// A snapshot or coordinator list that is not to be trusted.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum TrustError {
    #[error("{0}")]
    Signature(#[from] SignatureError),
    #[error("signed by a key that is not on the coordinator list")]
    UnknownSigner,
    #[error("version {offered} is not newer than version {current}")]
    NotNewer { offered: u64, current: u64 },
    #[error("signed by {signed} coordinators on the list; a change needs {needed}")]
    TooFewSignatures { signed: usize, needed: usize },
}

impl CoordinatorList {
    /// Checks that `snapshot` is signed by a coordinator on this list.
    pub fn check_snapshot(&self, snapshot: &Signed<NodeSnapshot>) -> Result<(), TrustError> {
        self.check_signed(snapshot)
    }

    /// Checks that `record`, such as a month's token key, is signed by a
    /// coordinator on this list.
    pub fn check_signed<T: SignedBody>(&self, record: &Signed<T>) -> Result<(), TrustError> {
        self.check_signer(record.signer)?;
        record.verify()?;
        Ok(())
    }

    /// The list that replaces this one, when `next` is newer and signed by
    /// more than half of the coordinators on this list.
    ///
    /// With one signature enough, one stolen key could replace all the other
    /// coordinators and then sign whatever node list it liked.
    pub fn successor(
        &self,
        next: &MultiSigned<CoordinatorList>,
    ) -> Result<CoordinatorList, TrustError> {
        if next.body.version <= self.version {
            return Err(TrustError::NotNewer {
                offered: next.body.version,
                current: self.version,
            });
        }
        let signed = next
            .valid_signers()
            .into_iter()
            .filter(|signer| self.keys.contains(&CoordinatorKey(*signer)))
            .count();
        let needed = self.majority();
        if signed < needed {
            return Err(TrustError::TooFewSignatures { signed, needed });
        }
        Ok(next.body.clone())
    }

    /// How many of the coordinators on this list make a majority. A key
    /// listed twice counts once, so listing a key again cannot weigh it more.
    fn majority(&self) -> usize {
        let distinct_keys: HashSet<[u8; 32]> = self.keys.iter().map(|key| key.0).collect();
        distinct_keys.len() / 2 + 1
    }

    fn check_signer(&self, signer: [u8; 32]) -> Result<(), TrustError> {
        if !self.keys.contains(&CoordinatorKey(signer)) {
            return Err(TrustError::UnknownSigner);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::SigningKey;

    use super::*;

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn record(node_key: &SigningKey) -> NodeRecord {
        NodeRecord {
            node_id: NodeId::of(&node_key.verifying_key().to_bytes()),
            url: "http://203.0.113.7:7001".into(),
            operator: "alice".into(),
            network: "AS3320".into(),
            region: "de".into(),
            capacity_bytes: 1 << 40,
            signed_at: 1_000,
        }
    }

    fn list_of(keys: &[&SigningKey]) -> CoordinatorList {
        CoordinatorList {
            version: 1,
            keys: keys
                .iter()
                .map(|key| CoordinatorKey(key.verifying_key().to_bytes()))
                .collect(),
        }
    }

    #[test]
    fn a_node_record_verifies_when_the_node_signed_it() {
        let signed = Signed::sign(&key(1), record(&key(1)));

        assert_eq!(signed.verify_node(), Ok(()));
    }

    #[test]
    fn a_node_cannot_sign_a_record_for_another_node() {
        let signed = Signed::sign(&key(2), record(&key(1)));

        assert!(matches!(
            signed.verify_node(),
            Err(NodeRecordError::WrongSigner { .. })
        ));
    }

    #[test]
    fn only_listed_coordinators_sign_snapshots() {
        let coordinator = key(10);
        let stranger = key(11);
        let list = list_of(&[&coordinator]);
        let snapshot = NodeSnapshot {
            published_at: 5,
            nodes: vec![ListedNode {
                record: Signed::sign(&key(1), record(&key(1))),
                uptime_permille: 990,
                audit_pass_permille: None,
                bandwidth_bits_per_second: None,
                used_bytes: None,
                takes_allowance: false,
                measured_network: None,
                measured_country: None,
                operator_id: None,
                down: false,
            }],
            daily_loss_per_million: None,
        };

        let genuine = Signed::sign(&coordinator, snapshot.clone());
        let foreign = Signed::sign(&stranger, snapshot);

        assert_eq!(list.check_snapshot(&genuine), Ok(()));
        assert_eq!(
            list.check_snapshot(&foreign),
            Err(TrustError::UnknownSigner)
        );
    }

    /// `list` with `version`.
    fn version(mut list: CoordinatorList, version: u64) -> CoordinatorList {
        list.version = version;
        list
    }

    #[test]
    fn the_only_coordinator_can_add_another() {
        let first = key(10);
        let second = key(11);
        let list = list_of(&[&first]);
        let next = version(list_of(&[&first, &second]), 2);

        let accepted = list.successor(&MultiSigned::sign(&[&first], next.clone()));

        assert_eq!(accepted, Ok(next));
    }

    #[test]
    fn a_change_needs_more_than_half_of_the_coordinators() {
        let keys = [key(10), key(11), key(12)];
        let list = list_of(&[&keys[0], &keys[1], &keys[2]]);
        let next = version(list_of(&[&keys[0], &keys[1]]), 2);

        let by_one = MultiSigned::sign(&[&keys[0]], next.clone());
        let by_two = MultiSigned::sign(&[&keys[0], &keys[1]], next.clone());

        assert_eq!(
            list.successor(&by_one),
            Err(TrustError::TooFewSignatures {
                signed: 1,
                needed: 2
            })
        );
        assert_eq!(list.successor(&by_two), Ok(next));
    }

    #[test]
    fn half_of_an_even_list_is_not_enough() {
        let keys = [key(10), key(11)];
        let list = list_of(&[&keys[0], &keys[1]]);
        let next = version(list_of(&[&keys[0]]), 2);

        let by_one = MultiSigned::sign(&[&keys[0]], next);

        assert_eq!(
            list.successor(&by_one),
            Err(TrustError::TooFewSignatures {
                signed: 1,
                needed: 2
            })
        );
    }

    #[test]
    fn outsiders_and_repeated_signatures_do_not_count() {
        let keys = [key(10), key(11), key(12)];
        let outsider = key(13);
        let list = list_of(&[&keys[0], &keys[1], &keys[2]]);
        let takeover = version(list_of(&[&keys[0], &outsider]), 2);

        let signed = MultiSigned::sign(&[&keys[0], &keys[0], &outsider], takeover);

        assert_eq!(
            list.successor(&signed),
            Err(TrustError::TooFewSignatures {
                signed: 1,
                needed: 2
            })
        );
    }

    #[test]
    fn a_new_list_must_have_a_higher_version() {
        let first = key(10);
        let list = list_of(&[&first]);

        assert_eq!(
            list.successor(&MultiSigned::sign(&[&first], list.clone())),
            Err(TrustError::NotNewer {
                offered: 1,
                current: 1
            })
        );
    }
}
