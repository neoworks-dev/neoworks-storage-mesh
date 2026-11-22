//! Getting storage tokens from the coordinator without it learning which
//! tokens it gave to whom.
//!
//! The device makes a random message per token, blinds each under the
//! month's public key, and sends only the blinded ones. The coordinator signs
//! them blind; unblinding turns each signature into one on the original
//! message, which the coordinator never saw. `mesh_core::tokens` describes
//! the requests.

use blind_rsa_signatures::{BlindSignature, DefaultRng, PublicKeySha384PSSRandomized};
use mesh_core::node_list::{CoordinatorList, TrustError};
use mesh_core::signed::Signed;
use mesh_core::tokens::{
    HexBytes, ISSUE_ROUTE, IssueRequest, IssueResponse, Token, TokenKey, TokenKind, token_key_path,
};
use reqwest::StatusCode;

use crate::node_client::NodeClient;

/// Why tokens could not be had.
#[derive(Debug, thiserror::Error)]
pub enum TokenError {
    #[error("the coordinator could not be reached: {0}")]
    Unreachable(#[from] reqwest::Error),
    #[error("the token key is not to be trusted: {0}")]
    Untrusted(#[from] TrustError),
    #[error("wrong email or password")]
    WrongPassword,
    #[error("the coordinator answered {status}: {message}")]
    Refused { status: StatusCode, message: String },
    #[error("the coordinator answered in a form this client does not understand")]
    UnexpectedAnswer,
}

/// The current month's token key of `kind`, once a coordinator on
/// `coordinators` is found to have signed it.
pub async fn token_key(
    client: &NodeClient,
    coordinator: &str,
    coordinators: &CoordinatorList,
    kind: TokenKind,
) -> Result<TokenKey, TokenError> {
    let url = format!(
        "{}{}",
        coordinator.trim_end_matches('/'),
        token_key_path(kind)
    );
    let response = client.http().get(url).send().await?;
    if !response.status().is_success() {
        return Err(refused(response).await);
    }
    let body = response.bytes().await?;
    let key: Signed<TokenKey> =
        serde_json::from_slice(&body).map_err(|_| TokenError::UnexpectedAnswer)?;
    coordinators.check_signed(&key)?;
    // A key of another kind would sign tokens the nodes take as this kind.
    if key.body.kind != kind {
        return Err(TokenError::UnexpectedAnswer);
    }
    Ok(key.body)
}

/// `count` new tokens of `kind` and this month for the account `email`,
/// proven with its `auth_key`: allowance tokens from its monthly allowance,
/// paid ones from its balance.
pub async fn get_tokens(
    client: &NodeClient,
    coordinator: &str,
    coordinators: &CoordinatorList,
    email: &str,
    auth_key: [u8; 32],
    kind: TokenKind,
    count: usize,
) -> Result<Vec<Token>, TokenError> {
    let key = token_key(client, coordinator, coordinators, kind).await?;
    let public_key = PublicKeySha384PSSRandomized::from_der(&key.public_key)
        .map_err(|_| TokenError::UnexpectedAnswer)?;
    let mut messages = Vec::with_capacity(count);
    let mut blindings = Vec::with_capacity(count);
    for _ in 0..count {
        let mut message = [0u8; 32];
        getrandom::fill(&mut message).expect("the operating system's random source works");
        let blinding = public_key
            .blind(&mut DefaultRng, message)
            .map_err(|_| TokenError::UnexpectedAnswer)?;
        messages.push(message);
        blindings.push(blinding);
    }
    let request = IssueRequest {
        email: email.to_string(),
        auth_key,
        month: key.month,
        kind,
        blinded: blindings
            .iter()
            .map(|blinding| HexBytes(blinding.blind_message.0.clone()))
            .collect(),
    };
    let answer = issue(client, coordinator, &request).await?;
    if answer.blind_signatures.len() != count {
        return Err(TokenError::UnexpectedAnswer);
    }
    let mut tokens = Vec::with_capacity(count);
    for ((message, blinding), blind_signature) in messages
        .into_iter()
        .zip(&blindings)
        .zip(answer.blind_signatures)
    {
        // Finalizing also checks the signature against the public key, so a
        // coordinator cannot hand out tokens that nodes would refuse.
        let signature = public_key
            .finalize(&BlindSignature(blind_signature.0), blinding, message)
            .map_err(|_| TokenError::UnexpectedAnswer)?;
        let randomizer = blinding
            .msg_randomizer
            .ok_or(TokenError::UnexpectedAnswer)?;
        tokens.push(Token {
            month: key.month,
            kind,
            message,
            randomizer: randomizer.0,
            signature: signature.0,
        });
    }
    Ok(tokens)
}

async fn issue(
    client: &NodeClient,
    coordinator: &str,
    request: &IssueRequest,
) -> Result<IssueResponse, TokenError> {
    let url = format!("{}{ISSUE_ROUTE}", coordinator.trim_end_matches('/'));
    // A request is plain strings and bytes, which always serialize.
    let body = serde_json::to_vec(request).expect("an issue request serializes");
    let response = client
        .http()
        .post(url)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await?;
    if response.status() == StatusCode::UNAUTHORIZED {
        return Err(TokenError::WrongPassword);
    }
    if !response.status().is_success() {
        return Err(refused(response).await);
    }
    let body = response.bytes().await?;
    serde_json::from_slice(&body).map_err(|_| TokenError::UnexpectedAnswer)
}

async fn refused(response: reqwest::Response) -> TokenError {
    let status = response.status();
    // The reason is for people reading the error; a body that can't be read
    // only loses that.
    let message = response.text().await.unwrap_or_default();
    TokenError::Refused { status, message }
}
