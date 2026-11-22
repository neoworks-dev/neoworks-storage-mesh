//! Random numbers from the operating system, for choices a node must not be
//! able to predict: which segments an audit asks for and where pieces go.

/// A uniformly random `u32`.
pub fn random_u32() -> u32 {
    // The operating system's random source also made the file's keys; a
    // device without one cannot have stored a file in the first place.
    getrandom::u32().expect("the operating system's random source works")
}

/// A uniformly random number in `[0, 1)`.
pub fn random_unit() -> f64 {
    let bits = getrandom::u64().expect("the operating system's random source works");
    // The top 53 bits fill an f64's mantissa exactly.
    (bits >> 11) as f64 / (1u64 << 53) as f64
}
