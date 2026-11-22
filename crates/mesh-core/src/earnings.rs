//! How node operators earn tokens for hosting.
//!
//! When a node spends a paid token, the coordinator holds the token's value
//! instead of crediting it at once, and pays it out to the node's operator
//! bit by bit over the month the tab lasts, for as long as the node stays
//! up and passes the coordinator's audits. What a node that went down or
//! failed its audits did not earn goes to repairing what it held. Only buying from a coordinator creates tokens; hosting moves them
//! from users to operators, and tokens never turn back into money.
//!
//! A node names its operator's account with a [`OperatorClaim`] signed with
//! its key:
//!
//! - `POST /nodes/operator` with a signed [`OperatorClaim`] in JSON links the
//!   node to the account, replacing any earlier claim, and answers
//!   `204 No Content`.

use serde::{Deserialize, Serialize};

use crate::NodeId;
use crate::signed::SignedBody;

/// The route a node names its operator at.
pub const OPERATOR_ROUTE: &str = "/nodes/operator";

/// A node saying which account its earnings go to.
///
/// Only the node's key can sign it, so nobody else can redirect a node's
/// earnings. The account does not have to agree: crediting it only ever
/// gives it tokens.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorClaim {
    pub node_id: NodeId,
    /// The email of the account the node's earnings are credited to.
    pub email: String,
    /// When the node signed the claim, in Unix seconds. A newer claim
    /// replaces an older one, so an old claim can't be replayed to move the
    /// earnings back.
    pub signed_at: u64,
}

impl SignedBody for OperatorClaim {
    const CONTEXT: &'static str = "neoworks-mesh operator claim v1";
}
