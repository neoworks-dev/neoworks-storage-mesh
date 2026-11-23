//! The tokens an account holds and has not handed to a node yet.
//!
//! The wallet is kept in the account's index ([`crate::index`]), one entry
//! per token, so every device of the account sees the same tokens. Two
//! devices can still take the same token at once; the node then finds it
//! spent, and the device takes the next one.

use mesh_core::tokens::{Token, TokenKind};

/// Tokens to pay nodes with.
#[derive(Debug, Default)]
pub struct Wallet {
    tokens: Vec<Token>,
}

impl Wallet {
    /// A wallet holding `tokens`.
    pub fn new(tokens: Vec<Token>) -> Self {
        Wallet { tokens }
    }

    /// The tokens held.
    pub fn tokens(&self) -> &[Token] {
        &self.tokens
    }

    /// Takes up to `count` tokens a node takes. Only nodes that take the
    /// allowance get allowance tokens, and they get those first, since the
    /// allowance is free and lapses with its month. Among tokens of one
    /// kind, the oldest month goes first, for the same reason.
    pub fn take(&mut self, count: usize, takes_allowance: bool) -> Vec<Token> {
        let preference = |token: &Token| {
            let kind_rank = match (token.kind, takes_allowance) {
                (TokenKind::Allowance, true) => 0,
                (TokenKind::Paid, _) => 1,
                (TokenKind::Allowance, false) => 2,
            };
            (kind_rank, token.month)
        };
        self.tokens.sort_by_key(preference);
        let usable = self
            .tokens
            .iter()
            .take_while(|token| takes_allowance || token.kind == TokenKind::Paid)
            .count();
        self.tokens.drain(..usable.min(count)).collect()
    }

    /// Puts back tokens that were taken but never reached a node.
    pub fn put_back(&mut self, tokens: Vec<Token>) {
        self.tokens.extend(tokens);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(kind: TokenKind, month: u64, byte: u8) -> Token {
        Token {
            month,
            kind,
            message: [byte; 32],
            randomizer: [0; 32],
            signature: Vec::new(),
        }
    }

    #[test]
    fn a_node_without_the_allowance_gets_only_paid_tokens() {
        let mut wallet = Wallet::new(vec![
            token(TokenKind::Allowance, 1, 1),
            token(TokenKind::Paid, 1, 2),
        ]);

        let taken = wallet.take(2, false);

        assert_eq!(taken, vec![token(TokenKind::Paid, 1, 2)]);
        assert_eq!(wallet.tokens().len(), 1);
    }

    #[test]
    fn a_node_with_the_allowance_gets_allowance_tokens_first_and_old_ones_first() {
        let mut wallet = Wallet::new(vec![
            token(TokenKind::Paid, 1, 1),
            token(TokenKind::Allowance, 2, 2),
            token(TokenKind::Allowance, 1, 3),
        ]);

        let taken = wallet.take(2, true);

        assert_eq!(
            taken,
            vec![
                token(TokenKind::Allowance, 1, 3),
                token(TokenKind::Allowance, 2, 2)
            ]
        );
        assert_eq!(wallet.take(5, true), vec![token(TokenKind::Paid, 1, 1)]);
    }
}
