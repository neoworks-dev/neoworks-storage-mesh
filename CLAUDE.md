## The project

An open source storage network (Apache 2.0). Devices encrypt and erasure-code
data, nodes store opaque pieces, coordinators track the network. `README.md`
describes the roles. People outside NeoWorks read and run this code, so it has
to make sense to someone opening it for the first time.

## Layout

One crate per role under `crates/`, each with a short `//!` doc at the top of
`lib.rs` saying what the crate is for:

- `mesh-core`: types shared by every role, such as piece ids. No I/O.
- `mesh-node`: the node, a library plus the `mesh-node` binary.
- `mesh-client`: the device side, a library plus the `mesh` binary.

A new role gets its own crate. A module is one file named after what it holds
(`piece_store.rs`, `erasure.rs`); avoid `utils.rs`, `helpers.rs` and `common.rs`.

## Code style

Code here is read far more often than it is written, so write the obvious
version rather than the clever one.

Names say what they hold, following Rust's conventions: `piece_id`,
`node_address`, `PieceStore`. No abbreviations beyond the ones everybody
knows (`id`, `len`, `url`).

Spell control flow out. Prefer `match` and `if let` with early returns over
long chains of combinators; use `?`, `map` and `ok_or` where the explicit form
would only repeat itself.

```rust
let Some(piece) = store.get(&piece_id)? else {
    return Ok(Response::NotFound);
};
```

Keep function bodies at most two levels deep, with guard clauses and early
returns keeping the main path flat. Extract a helper when a piece has its own
name and job; a fragment cut out only to satisfy the limit makes the code
harder to follow.

Each function does one thing and stays short. A value used once is written
where it is used. Hoist it only when that saves repeated work or names a
number that would otherwise be magic, like `const DATA_SHARDS: usize = 4`.

## Annotation

Every module starts with a `//!` comment saying what it holds and how it fits
the system. Every public item has a `///` doc comment. Inside functions,
comments explain what skimming the code won't tell you, in technical terms:
why a check exists, what an attacker could do without it, what a number was
chosen for. They describe the code as it is now; git holds the history.

```rust
// The id is the hash of the bytes, so a node cannot hand back different
// bytes under the same id without the client noticing.
if PieceId::of(&bytes) != piece_id {
    return Err(FetchError::WrongBytes);
}
```

Errors are typed enums with `thiserror` in libraries; binaries may use
`anyhow`. No `unwrap` or `expect` outside tests, except where an invariant
makes failure impossible, and then a comment says which invariant.

A refactor keeps behaviour, public APIs, data shapes, validation rules and side
effects exactly as they were. When it can't, ask before changing them.

Copy the surface conventions of surrounding code: formatting (`cargo fmt`),
import order, naming. Its structure is no precedent. When an existing pattern
looks like a mistake, ask me or check the documentation of the crate involved.

Edit files with Read, Write and Edit rather than sed, awk or python.

## Tests

Run checks and tests once a stretch of work is done, before committing:

1. Add a test for every behaviour change and bug fix: unit tests next to the
   code in a `#[cfg(test)] mod tests`, tests across crates or over the network
   in the crate's `tests/`. Refactors and style edits need none; say so in one
   sentence.
2. Run `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` and
   `cargo test`, whether or not you wrote a test.
3. When a test fails, fix the code. Change the test only when the test itself
   is wrong, and then say why.

## Git

If the tree is dirty when you start, commit what's there first, so your changes
stand alone. Write a short title, and add a body when the title doesn't carry
the change. Ask if you're unsure what to write. On a feature branch, commit
only what that branch is for. Commits end with their last sentence: no
`Co-Authored-By` or "Generated with Claude Code" lines.

## GitHub

The repository is `neoworks-dev/neoworks-storage-mesh`. Use plain `gh` for
everything here, reads and writes; `gh bot` is not used in this repo.

## Writing

Commits and issues carry only what matters: say the thing, explain what a
reader won't see for themselves, and stop.

Write issues and their comments as you'd explain them to a colleague, in
complete sentences.

- Open with a 2–3 sentence summary of what happened, why, and the fix.
- Use short headings with short paragraphs of prose under them.
- Join ideas with words: "first X, then Y" rather than "X → Y", and "A and B"
  rather than "A / B".
- Use at most two code identifiers per sentence, and say what each one does the
  first time it appears.
- Use bullets for genuinely separate items, each a full sentence, and tables
  for numbers or timelines.
- Write dates as 2026-10-07 18:28.
- Put error messages, paths and commands in code formatting, and long logs in a
  collapsed `<details>` block.
- End with a short "Out of scope" section for side findings.

## How we work

I want to stay on top of everything that happens in this repo, so the work is
coordinated with me as it goes rather than done around me. One agent at a time,
with me giving feedback. Keep each step small enough for me to read in one
sitting, and stop to show me before the work piles up.

When the next step is a decision (an approach, a fix for something unexpected,
cleaning up or deleting something, splitting work up), bring it to me with
your recommendation and let me choose. When something turns out different from
what we expected, tell me before working around it.

`main` is what I've reviewed. Typos, one-liners, small fixes and changes to how
we work (this file, `.claude/`, editor config) go straight to `main`.
Everything else gets a feature branch, named after its issue:

```sh
git switch -c 12-piece-leases main   # issue 12
```

Before starting, check whether the work is already half-built with
`git branch`, and read what's on the branch. Review happens here on the diff,
so open a pull request only when I ask.

## Issues

Work that won't finish in this session gets an issue, however loosely defined,
and so does anything concrete found along the way that isn't what we're doing
now. Write them as soon as the list exists.

Every issue gets a type, priority and effort (GitHub's issue type and the
organization's issue fields), and one area label:

- **Type**: `Bug` broken, `Feature` new, `Task` everything else.
- **Priority**: `Urgent` drop everything, `High` next up, `Medium` normal,
  `Low` nice to have.
- **Effort**: `Low` an hour or two, `Medium` about a day, `High` several days
  or needs splitting.
- **Area**: `area:core`, `area:node`, `area:client`, `area:coordinator`,
  `area:infra` (CI, tooling, packaging). An issue spanning three areas should
  be split.
