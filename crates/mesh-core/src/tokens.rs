//! Storage tokens: proof that someone paid for storage, which does not say
//! who.
//!
//! A token is a unit of value worth 0.1 euro cents. Nodes charge for storage
//! in tokens at the price coordinators publish for them
//! ([`crate::prices`]). Tokens are bought from a coordinator or earned by
//! hosting, and can never be turned back into money.
//!
//! There are two kinds. [`TokenKind::Paid`] tokens come out of an account's
//! balance and are taken by any node. [`TokenKind::Allowance`] tokens are the
//! free storage a coordinator gives each signed-in account every month, and
//! only the coordinator's own nodes take them: the allowance is storage on
//! those nodes, not tokens anyone could collect with fake accounts and spend
//! elsewhere.
//!
//! Each month the coordinator has an RSA key per kind for blind signatures
//! (RFC 9474, RSABSSA-SHA384-PSS-Randomized). An account asks for tokens by
//! sending blinded random messages; the coordinator signs them and learns
//! only how many. The device unblinds the signatures into tokens, and later
//! hands them to nodes, which cannot link a token to the account it was
//! issued to. Each token is spent once, at the coordinator, by the node it
//! was handed to, which signs the request with its key; the coordinator
//! keeps the list of spent tokens.
//!
//! A token is valid in its month and the next, so tokens bought near the end
//! of a month are not lost. A month's public key is signed with the
//! coordinator's Ed25519 key, so devices and nodes check it against the
//! coordinator list like the node list.
//!
//! - `GET /tokens/key/{kind}` returns the current month's signed [`TokenKey`]
//!   of that kind.
//! - `GET /tokens/key/{kind}/{month}` returns that month's, while it is
//!   valid.
//! - `POST /tokens/issue` with an [`IssueRequest`] returns an
//!   [`IssueResponse`], or `403 Forbidden` past the allowance or balance.
//! - `POST /tokens/spend` with a [`SpendRequest`] signed by the node returns
//!   a [`SpendResponse`].

use blind_rsa_signatures::{MessageRandomizer, PublicKeySha384PSSRandomized, Signature};
use serde::{Deserialize, Serialize};

use crate::NodeId;
use crate::signed::SignedBody;

/// The route of the current month's token key of a kind, in the router
/// syntax of `axum`.
pub const TOKEN_KEY_ROUTE: &str = "/tokens/key/{kind}";
/// The route of one month's token key of a kind, in the router syntax of
/// `axum`.
pub const MONTH_TOKEN_KEY_ROUTE: &str = "/tokens/key/{kind}/{month}";
/// The route that issues tokens.
pub const ISSUE_ROUTE: &str = "/tokens/issue";
/// The route that spends a token.
pub const SPEND_ROUTE: &str = "/tokens/spend";

/// How long a token month is. Months of equal length keep every role's idea
/// of which month it is the same, whatever the calendar says.
pub const MONTH_SECONDS: u64 = 30 * 24 * 60 * 60;

/// Millionths of a token, the unit tabs count in so that small pieces can be
/// charged what they cost.
pub const MICROTOKENS_PER_TOKEN: u64 = 1_000_000;

/// The most tokens one issue request may ask for.
pub const MAX_ISSUE_PER_REQUEST: usize = 1000;

/// The token month `unix_seconds` falls in.
pub fn month_of(unix_seconds: u64) -> u64 {
    unix_seconds / MONTH_SECONDS
}

/// Where a token came from, which decides which nodes take it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TokenKind {
    /// Bought or earned; every node takes it.
    Paid,
    /// The free monthly allowance; only the coordinator's own nodes take it.
    Allowance,
}

impl TokenKind {
    /// The kind as it appears in routes.
    pub fn as_str(self) -> &'static str {
        match self {
            TokenKind::Paid => "paid",
            TokenKind::Allowance => "allowance",
        }
    }
}

/// The path of the current month's token key of `kind`.
pub fn token_key_path(kind: TokenKind) -> String {
    format!("/tokens/key/{}", kind.as_str())
}

/// A month's public key for token signatures of one kind.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenKey {
    pub month: u64,
    pub kind: TokenKind,
    /// The RSA public key in DER.
    #[serde(with = "hex")]
    pub public_key: Vec<u8>,
}

impl SignedBody for TokenKey {
    const CONTEXT: &'static str = "neoworks-mesh token key v2";
}

/// Asks for tokens, as a signed-in account.
#[derive(Debug, Serialize, Deserialize)]
pub struct IssueRequest {
    pub email: String,
    /// The auth key the account signs in with (`mesh_core::sign_in`).
    #[serde(with = "hex")]
    pub auth_key: [u8; 32],
    pub month: u64,
    pub kind: TokenKind,
    /// The blinded messages, one per token.
    pub blinded: Vec<HexBytes>,
}

/// The blind signatures, in the order asked.
#[derive(Debug, Serialize, Deserialize)]
pub struct IssueResponse {
    pub blind_signatures: Vec<HexBytes>,
}

/// Bytes written as hex in JSON.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HexBytes(#[serde(with = "hex")] pub Vec<u8>);

/// A spendable token: a random message and the coordinator's signature on
/// it, which the coordinator never saw together.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Token {
    pub month: u64,
    pub kind: TokenKind,
    #[serde(with = "hex")]
    pub message: [u8; 32],
    /// The randomizer RFC 9474's randomized variant mixes into the message.
    #[serde(with = "hex")]
    pub randomizer: [u8; 32],
    #[serde(with = "hex")]
    pub signature: Vec<u8>,
}

impl Token {
    /// Whether the token carries a valid signature of the month key
    /// `public_key`, in DER. Says nothing about whether it was spent.
    pub fn is_signed_by(&self, public_key: &[u8]) -> bool {
        let Ok(key) = PublicKeySha384PSSRandomized::from_der(public_key) else {
            return false;
        };
        key.verify(
            &Signature(self.signature.clone()),
            Some(MessageRandomizer(self.randomizer)),
            self.message,
        )
        .is_ok()
    }
}

/// A node spending a token it was paid with, signed with the node's key so
/// the coordinator knows which node took it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpendRequest {
    pub node_id: NodeId,
    pub token: Token,
}

impl SignedBody for SpendRequest {
    const CONTEXT: &'static str = "neoworks-mesh spend request v1";
}

/// The answer to spending a token.
#[derive(Debug, Serialize, Deserialize)]
pub struct SpendResponse {
    /// `true` when this request spent it, `false` when it was spent before.
    pub spent_now: bool,
}
