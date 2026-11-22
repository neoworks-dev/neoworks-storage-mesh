//! The one spelling of a 32-byte value in URLs, file names and JSON: 64
//! lowercase hex digits.

/// The 32 bytes `text` spells, or `None` unless it is 64 lowercase hex digits.
///
/// Only the canonical form is accepted, so every value has exactly one
/// spelling. Nodes use these spellings as file names and keys, and two
/// spellings of one value would be two entries.
pub(crate) fn parse_canonical_hex(text: &str) -> Option<[u8; 32]> {
    let is_canonical = text.len() == 64
        && text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
    if !is_canonical {
        return None;
    }
    let mut bytes = [0u8; 32];
    hex::decode_to_slice(text, &mut bytes).ok()?;
    Some(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_only_64_lowercase_hex_digits() {
        let text = "ab".repeat(32);

        assert_eq!(parse_canonical_hex(&text), Some([0xab; 32]));
        assert_eq!(parse_canonical_hex(&text.to_uppercase()), None);
        assert_eq!(parse_canonical_hex(&text[1..]), None);
        assert_eq!(parse_canonical_hex("../etc/passwd"), None);
    }
}
