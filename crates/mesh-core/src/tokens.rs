//! Storage tokens: proof that someone paid for storage, which does not say
//! who.
//!
//! Each month the coordinator has an RSA key for blind signatures (RFC 9474,
//! RSABSSA-SHA384-PSS-Randomized). An account asks for its monthly allowance
//! by sending blinded random messages; the coordinator signs them and learns
//! only how many. The device unblinds the signatures into tokens, and later
//! hands them to nodes, which cannot link a token to the account it was
//! issued to. Each token is spent once, at the coordinator, which keeps the
//! list of spent tokens.
//!
//! A token is valid in its month and the next, so tokens bought near the end
//! of a month are not lost. The month's public key is signed with the
//! coordinator's Ed25519 key, so devices and nodes check it against the
//! coordinator list like the node list.
//!
//! - `GET /tokens/key` returns the current month's signed [`TokenKey`].
//! - `GET /tokens/key/{month}` returns that month's, while it is valid.
//! - `POST /tokens/issue` with an [`IssueRequest`] returns an
//!   [`IssueResponse`], or `403 Forbidden` past the allowance.
//! - `POST /tokens/spend` with a [`Token`] returns a [`SpendResponse`].

use blind_rsa_signatures::{MessageRandomizer, PublicKeySha384PSSRandomized, Signature};
use serde::{Deserialize, Serialize};

use crate::signed::SignedBody;

/// The route of the current month's token key.
pub const TOKEN_KEY_ROUTE: &str = "/tokens/key";
/// The route of one month's token key, in the router syntax of `axum`.
pub const MONTH_TOKEN_KEY_ROUTE: &str = "/tokens/key/{month}";
/// The route that issues tokens.
pub const ISSUE_ROUTE: &str = "/tokens/issue";
/// The route that spends a token.
pub const SPEND_ROUTE: &str = "/tokens/spend";

/// How long a token month is. Months of equal length keep every role's idea
/// of which month it is the same, whatever the calendar says.
pub const MONTH_SECONDS: u64 = 30 * 24 * 60 * 60;

/// How much storage one token pays for, for one month: 100 MB, the current
/// guess for a useful unit.
pub const TOKEN_BYTES: u64 = 100_000_000;

/// The most tokens one issue request may ask for.
pub const MAX_ISSUE_PER_REQUEST: usize = 1000;

/// The token month `unix_seconds` falls in.
pub fn month_of(unix_seconds: u64) -> u64 {
    unix_seconds / MONTH_SECONDS
}

/// A month's public key for token signatures.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenKey {
    pub month: u64,
    /// The RSA public key in DER.
    #[serde(with = "hex")]
    pub public_key: Vec<u8>,
}

impl SignedBody for TokenKey {
    const CONTEXT: &'static str = "neoworks-mesh token key v1";
}

/// Asks for tokens, as a signed-in account.
#[derive(Debug, Serialize, Deserialize)]
pub struct IssueRequest {
    pub email: String,
    /// The auth key the account signs in with (`mesh_core::sign_in`).
    #[serde(with = "hex")]
    pub auth_key: [u8; 32],
    pub month: u64,
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

/// The answer to spending a token.
#[derive(Debug, Serialize, Deserialize)]
pub struct SpendResponse {
    /// `true` when this request spent it, `false` when it was spent before.
    pub spent_now: bool,
}
