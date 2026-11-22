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
//! with. It is versioned and each new version is signed by a coordinator of
//! the last one, so coordinators can be added without a protocol change.

use serde::{Deserialize, Serialize};

use crate::NodeId;
use crate::signed::{SignatureError, Signed, SignedBody};

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
}

/// Every node a coordinator knows, at one moment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSnapshot {
    /// When the snapshot was made, in Unix seconds. Clients keep the newest
    /// they have seen, so an old snapshot cannot be replayed to them.
    pub published_at: u64,
    pub nodes: Vec<ListedNode>,
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

    /// The list that replaces this one, when `next` is newer and signed by a
    /// coordinator on this list.
    ///
    /// One signature is enough while the list holds one key. Once there are
    /// several coordinators, a change should need most of them, so that one
    /// stolen key cannot replace the others.
    pub fn successor(&self, next: &Signed<CoordinatorList>) -> Result<CoordinatorList, TrustError> {
        self.check_signer(next.signer)?;
        next.verify()?;
        if next.body.version <= self.version {
            return Err(TrustError::NotNewer {
                offered: next.body.version,
                current: self.version,
            });
        }
        Ok(next.body.clone())
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
            }],
        };

        let genuine = Signed::sign(&coordinator, snapshot.clone());
        let foreign = Signed::sign(&stranger, snapshot);

        assert_eq!(list.check_snapshot(&genuine), Ok(()));
        assert_eq!(
            list.check_snapshot(&foreign),
            Err(TrustError::UnknownSigner)
        );
    }

    #[test]
    fn a_listed_coordinator_can_add_another() {
        let first = key(10);
        let second = key(11);
        let list = list_of(&[&first]);
        let mut next = list_of(&[&first, &second]);
        next.version = 2;

        let accepted = list.successor(&Signed::sign(&first, next.clone()));

        assert_eq!(accepted, Ok(next));
    }

    #[test]
    fn a_list_must_be_newer_and_signed_by_a_listed_coordinator() {
        let first = key(10);
        let outsider = key(12);
        let list = list_of(&[&first]);
        let mut takeover = list_of(&[&outsider]);
        takeover.version = 2;

        assert_eq!(
            list.successor(&Signed::sign(&outsider, takeover)),
            Err(TrustError::UnknownSigner)
        );
        assert_eq!(
            list.successor(&Signed::sign(&first, list.clone())),
            Err(TrustError::NotNewer {
                offered: 1,
                current: 1
            })
        );
    }
}
