//! The history of a collection, such as a calendar or the file index, as a
//! graph of commits like git's.
//!
//! Every save is a [`Commit`]: the changes of one action and the commits it
//! follows. A commit is stored in the mesh like any file and named by the
//! hash of its bytes, so it never changes once written. Each device keeps the
//! whole graph and the entries it adds up to; nodes only ever see encrypted
//! commits and are never asked about entries.
//!
//! Entries are addressed by an id, and their fields by a number, the
//! OpenSchema ordinal, so renaming a field never changes history. This module
//! holds the graph and replays it; `commits` stores and fetches commits.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::manifest::Manifest;

/// The BLAKE3 hash of a commit's stored bytes.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CommitId(#[serde(with = "hex")] pub [u8; 32]);

impl CommitId {
    /// The id of a commit stored as `bytes`.
    pub fn of(bytes: &[u8]) -> Self {
        CommitId(*blake3::hash(bytes).as_bytes())
    }
}

impl fmt::Debug for CommitId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "CommitId({})", hex::encode(self.0))
    }
}

/// How to find a commit: its id, and the manifest of the object it is stored
/// as. The manifest holds the commit's keys, so a reference is as secret as
/// the commit itself.
#[derive(Clone, Serialize, Deserialize)]
pub struct CommitRef {
    pub id: CommitId,
    pub manifest: Manifest,
}

/// One change to the entries of a collection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "change", rename_all = "snake_case")]
pub enum Change {
    /// Sets one field of an entry, creating the entry if needed.
    Set {
        entry: String,
        field: u32,
        value: serde_json::Value,
    },
    /// Removes one field of an entry; an entry left without fields is gone.
    Unset { entry: String, field: u32 },
    /// Removes an entry and all its fields.
    Remove { entry: String },
}

/// The changes of one action and the commits it follows.
#[derive(Clone, Serialize, Deserialize)]
pub struct Commit {
    /// None for the first commit of a collection, one for a plain save, two
    /// for a merge.
    pub parents: Vec<CommitRef>,
    /// The device that made the commit, which breaks ties when two devices
    /// changed the same field at the same second.
    pub device: String,
    /// When the commit was made, in Unix seconds on the device's clock.
    pub made_at: u64,
    pub changes: Vec<Change>,
}

/// The entries of a collection: each entry's fields by their ordinal.
pub type Entries = BTreeMap<String, BTreeMap<u32, serde_json::Value>>;

/// A commit that is needed but not in the graph.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("commit {0:?} is not in the graph")]
pub struct MissingCommit(pub CommitId);

/// The commits a device knows, by id.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Graph {
    commits: HashMap<CommitId, Commit>,
}

impl Graph {
    /// Adds `commit` under `id`. The caller has checked that `id` is the hash
    /// of the bytes the commit was stored as.
    pub fn insert(&mut self, id: CommitId, commit: Commit) {
        self.commits.insert(id, commit);
    }

    pub fn contains(&self, id: &CommitId) -> bool {
        self.commits.contains_key(id)
    }

    pub fn get(&self, id: &CommitId) -> Option<&Commit> {
        self.commits.get(id)
    }

    /// How many commits the graph holds.
    pub fn len(&self) -> usize {
        self.commits.len()
    }

    pub fn is_empty(&self) -> bool {
        self.commits.is_empty()
    }

    /// Every commit `head` follows, and `head` itself, each after all of its
    /// parents.
    ///
    /// Parents are visited in the order the commit lists them, so every
    /// device puts the same history in the same order.
    pub fn history(&self, head: &CommitId) -> Result<Vec<CommitId>, MissingCommit> {
        let mut ordered = Vec::new();
        let mut visited = HashSet::new();
        // Each step is a commit and whether its parents are already ordered;
        // an explicit stack instead of recursion, since histories run to
        // many thousands of commits.
        let mut stack = vec![(*head, false)];
        while let Some((id, parents_done)) = stack.pop() {
            if parents_done {
                ordered.push(id);
                continue;
            }
            if !visited.insert(id) {
                continue;
            }
            let commit = self.get(&id).ok_or(MissingCommit(id))?;
            stack.push((id, true));
            for parent in commit.parents.iter().rev() {
                stack.push((parent.id, false));
            }
        }
        Ok(ordered)
    }

    /// The entries the history up to `head` adds up to.
    pub fn entries_at(&self, head: &CommitId) -> Result<Entries, MissingCommit> {
        let mut entries = Entries::new();
        for id in self.history(head)? {
            let commit = self.get(&id).ok_or(MissingCommit(id))?;
            apply(&mut entries, &commit.changes);
        }
        Ok(entries)
    }
}

/// Applies `changes` to `entries`, in order.
pub fn apply(entries: &mut Entries, changes: &[Change]) {
    for change in changes {
        match change {
            Change::Set {
                entry,
                field,
                value,
            } => {
                entries
                    .entry(entry.clone())
                    .or_default()
                    .insert(*field, value.clone());
            }
            Change::Unset { entry, field } => unset(entries, entry, *field),
            Change::Remove { entry } => {
                entries.remove(entry);
            }
        }
    }
}

/// Removes `field` from `entry`, and the entry once it has no fields left, so
/// an entry with no fields and an absent entry are the same state.
fn unset(entries: &mut Entries, entry: &str, field: u32) {
    let Some(fields) = entries.get_mut(entry) else {
        return;
    };
    fields.remove(&field);
    if fields.is_empty() {
        entries.remove(entry);
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn reference(id: u8) -> CommitRef {
        CommitRef {
            id: CommitId([id; 32]),
            manifest: Manifest {
                size: 0,
                chunks: Vec::new(),
            },
        }
    }

    fn commit(parents: &[u8], changes: Vec<Change>) -> Commit {
        Commit {
            parents: parents.iter().map(|&id| reference(id)).collect(),
            device: "laptop".into(),
            made_at: 0,
            changes,
        }
    }

    fn set(entry: &str, field: u32, value: serde_json::Value) -> Change {
        Change::Set {
            entry: entry.into(),
            field,
            value,
        }
    }

    #[test]
    fn history_puts_parents_before_children() {
        let mut graph = Graph::default();
        graph.insert(CommitId([1; 32]), commit(&[], vec![]));
        graph.insert(CommitId([2; 32]), commit(&[1], vec![]));
        graph.insert(CommitId([3; 32]), commit(&[1], vec![]));
        graph.insert(CommitId([4; 32]), commit(&[2, 3], vec![]));

        let history = graph.history(&CommitId([4; 32])).unwrap();

        let ids: Vec<u8> = history.iter().map(|id| id.0[0]).collect();
        assert_eq!(ids, [1, 2, 3, 4]);
    }

    #[test]
    fn entries_replay_every_change_in_order() {
        let mut graph = Graph::default();
        graph.insert(
            CommitId([1; 32]),
            commit(&[], vec![set("dentist", 1, json!("Tuesday"))]),
        );
        graph.insert(
            CommitId([2; 32]),
            commit(
                &[1],
                vec![
                    set("dentist", 1, json!("Friday")),
                    set("dentist", 2, json!("bring card")),
                    set("party", 1, json!("Saturday")),
                ],
            ),
        );
        graph.insert(
            CommitId([3; 32]),
            commit(
                &[2],
                vec![Change::Remove {
                    entry: "party".into(),
                }],
            ),
        );

        let entries = graph.entries_at(&CommitId([3; 32])).unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries["dentist"][&1], json!("Friday"));
        assert_eq!(entries["dentist"][&2], json!("bring card"));
    }

    #[test]
    fn a_missing_parent_is_reported() {
        let mut graph = Graph::default();
        graph.insert(CommitId([2; 32]), commit(&[1], vec![]));

        assert_eq!(
            graph.history(&CommitId([2; 32])),
            Err(MissingCommit(CommitId([1; 32])))
        );
    }
}
