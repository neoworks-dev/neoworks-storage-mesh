//! Bringing two heads of a collection together with a three-way merge.
//!
//! When two devices changed a collection while apart, each has a head the
//! other lacks. The merge compares each side with what both share, field by
//! field, so only a field both sides changed is a real conflict:
//!
//! - A field only one side changed takes that side's value.
//! - A field both sides changed takes the value of the later change; the
//!   device id breaks ties, and the value itself if one device changed it
//!   twice in the same second. The losing value is reported next to the
//!   result, so an app can show that something was changed on two devices.
//!
//! The merge commit holds the parents, sorted by id, and the changes that
//! turn both histories replayed into the merged entries. Its device is empty
//! and its time is the later head's. Nothing in it depends on which device
//! merged, so two devices merging the same two heads write the same bytes,
//! the same commit id, and have nothing left to merge between them.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde_json::Value;

use crate::version_graph::{
    Change, Commit, CommitId, CommitRef, Entries, Graph, MissingCommit, apply,
};

/// When a field was last changed on one side: the commit's time and device.
type Stamp = (u64, String);

/// A field both sides changed, and what each wanted.
#[derive(Clone, Debug, PartialEq)]
pub struct Conflict {
    pub entry: String,
    pub field: u32,
    /// The value the merge kept; `None` when it keeps the field removed.
    pub kept: Option<Value>,
    /// The value of the side that lost.
    pub lost: Option<Value>,
}

/// What merging two heads gives.
pub enum MergeOutcome {
    /// One head already follows the other, so it is the result as it is.
    FastForward(CommitRef),
    /// A new merge commit, not stored yet, the entries it gives and the
    /// fields both sides changed.
    Merged {
        commit: Commit,
        entries: Entries,
        conflicts: Vec<Conflict>,
    },
}

/// Merges the heads `one` and `other` of a collection in `graph`.
pub fn merge(
    graph: &Graph,
    one: &CommitRef,
    other: &CommitRef,
) -> Result<MergeOutcome, MissingCommit> {
    // Sorting the heads makes every step below the same whichever device
    // merges and whichever head it calls its own.
    let (first, second) = if one.id <= other.id {
        (one, other)
    } else {
        (other, one)
    };
    let first_history = graph.history(&first.id)?;
    let second_history = graph.history(&second.id)?;
    let first_reach: HashSet<CommitId> = first_history.iter().copied().collect();
    let second_reach: HashSet<CommitId> = second_history.iter().copied().collect();
    if first_reach.contains(&second.id) {
        return Ok(MergeOutcome::FastForward(first.clone()));
    }
    if second_reach.contains(&first.id) {
        return Ok(MergeOutcome::FastForward(second.clone()));
    }
    let shared: Vec<CommitId> = first_history
        .iter()
        .filter(|id| second_reach.contains(id))
        .copied()
        .collect();
    let base = replay(graph, &shared)?;
    let first_side = Side::of(graph, &first_history, &second_reach)?;
    let second_side = Side::of(graph, &second_history, &first_reach)?;

    let (entries, conflicts) = merge_entries(&base, &first_side, &second_side);
    // Replaying a merge commit's history runs the first parent's history,
    // then what only the second has, then the merge commit itself; its
    // changes have to start from exactly that state.
    let mut combined = first_history.clone();
    combined.extend(second_history.iter().filter(|id| !first_reach.contains(id)));
    let replayed = replay(graph, &combined)?;
    let first_commit = graph.get(&first.id).ok_or(MissingCommit(first.id))?;
    let second_commit = graph.get(&second.id).ok_or(MissingCommit(second.id))?;
    let commit = Commit {
        parents: vec![first.clone(), second.clone()],
        device: String::new(),
        made_at: first_commit.made_at.max(second_commit.made_at),
        changes: changes_between(&replayed, &entries),
    };
    Ok(MergeOutcome::Merged {
        commit,
        entries,
        conflicts,
    })
}

/// One side of a merge: the entries at its head, and when each field was last
/// changed by the commits only that side has.
struct Side {
    entries: Entries,
    stamps: BTreeMap<(String, u32), Stamp>,
    /// Entry removals, which touch every field of the entry.
    removals: BTreeMap<String, Stamp>,
}

impl Side {
    /// The side whose history is `history`, apart from the commits in
    /// `other_reach`.
    fn of(
        graph: &Graph,
        history: &[CommitId],
        other_reach: &HashSet<CommitId>,
    ) -> Result<Side, MissingCommit> {
        let mut side = Side {
            entries: replay(graph, history)?,
            stamps: BTreeMap::new(),
            removals: BTreeMap::new(),
        };
        for id in history.iter().filter(|id| !other_reach.contains(id)) {
            let commit = graph.get(id).ok_or(MissingCommit(*id))?;
            let stamp = (commit.made_at, commit.device.clone());
            for change in &commit.changes {
                side.note(change, &stamp);
            }
        }
        Ok(side)
    }

    /// Notes that `change` happened at `stamp`. History runs oldest first, so
    /// a later change of the same field overwrites an earlier one.
    fn note(&mut self, change: &Change, stamp: &Stamp) {
        match change {
            Change::Set { entry, field, .. } | Change::Unset { entry, field } => {
                self.stamps.insert((entry.clone(), *field), stamp.clone());
            }
            Change::Remove { entry } => {
                self.removals.insert(entry.clone(), stamp.clone());
            }
        }
    }

    /// When this side last changed `field` of `entry`, by setting it or by
    /// removing the entry.
    fn stamp(&self, entry: &str, field: u32) -> Option<&Stamp> {
        let field_stamp = self.stamps.get(&(entry.to_string(), field));
        let removal_stamp = self.removals.get(entry);
        field_stamp.max(removal_stamp)
    }

    fn value(&self, entry: &str, field: u32) -> Option<&Value> {
        self.entries.get(entry)?.get(&field)
    }
}

/// The merged entries of `base` changed by `first` and `second`, and the
/// fields both changed.
fn merge_entries(base: &Entries, first: &Side, second: &Side) -> (Entries, Vec<Conflict>) {
    let mut merged = Entries::new();
    let mut conflicts = Vec::new();
    for (entry, field) in all_fields(&[base, &first.entries, &second.entries]) {
        let base_value = base.get(&entry).and_then(|fields| fields.get(&field));
        let first_value = first.value(&entry, field);
        let second_value = second.value(&entry, field);
        let kept = if first_value == second_value || second_value == base_value {
            first_value
        } else if first_value == base_value {
            second_value
        } else {
            let (kept, lost) = pick_later(first, second, &entry, field);
            conflicts.push(Conflict {
                entry: entry.clone(),
                field,
                kept: kept.cloned(),
                lost: lost.cloned(),
            });
            kept
        };
        if let Some(value) = kept {
            merged
                .entry(entry)
                .or_default()
                .insert(field, value.clone());
        }
    }
    (merged, conflicts)
}

/// The value of the side that changed `field` later, then the other's.
fn pick_later<'side>(
    first: &'side Side,
    second: &'side Side,
    entry: &str,
    field: u32,
) -> (Option<&'side Value>, Option<&'side Value>) {
    let first_value = first.value(entry, field);
    let second_value = second.value(entry, field);
    // Stamps compare by time, then device. Should both be equal, one device
    // changed the field twice in one second on two branches; the values
    // themselves then decide, so the choice still never depends on who merges.
    let first_key = (first.stamp(entry, field), first_value.map(Value::to_string));
    let second_key = (
        second.stamp(entry, field),
        second_value.map(Value::to_string),
    );
    if first_key >= second_key {
        (first_value, second_value)
    } else {
        (second_value, first_value)
    }
}

/// Every (entry, field) present in any of `sets`, in order.
fn all_fields(sets: &[&Entries]) -> BTreeSet<(String, u32)> {
    sets.iter()
        .flat_map(|entries| entries.iter())
        .flat_map(|(entry, fields)| fields.keys().map(move |field| (entry.clone(), *field)))
        .collect()
}

/// The entries the commits `ids` add up to, applied in that order.
fn replay(graph: &Graph, ids: &[CommitId]) -> Result<Entries, MissingCommit> {
    let mut entries = Entries::new();
    for id in ids {
        let commit = graph.get(id).ok_or(MissingCommit(*id))?;
        apply(&mut entries, &commit.changes);
    }
    Ok(entries)
}

/// The changes that turn `from` into `to`, in a fixed order.
fn changes_between(from: &Entries, to: &Entries) -> Vec<Change> {
    let mut changes = Vec::new();
    for (entry, field) in all_fields(&[from, to]) {
        let old = from.get(&entry).and_then(|fields| fields.get(&field));
        let new = to.get(&entry).and_then(|fields| fields.get(&field));
        if old == new {
            continue;
        }
        let change = match new {
            Some(value) => Change::Set {
                entry,
                field,
                value: value.clone(),
            },
            None => Change::Unset { entry, field },
        };
        changes.push(change);
    }
    changes
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::manifest::Manifest;

    /// Builds graphs by hand, with ids counted up from 1.
    struct Builder {
        graph: Graph,
        next: u8,
    }

    impl Builder {
        fn new() -> Builder {
            Builder {
                graph: Graph::default(),
                next: 1,
            }
        }

        fn commit(
            &mut self,
            parents: &[&CommitRef],
            device: &str,
            made_at: u64,
            changes: Vec<Change>,
        ) -> CommitRef {
            let reference = CommitRef {
                id: CommitId([self.next; 32]),
                manifest: Manifest {
                    size: 0,
                    chunks: Vec::new(),
                },
            };
            self.next += 1;
            let commit = Commit {
                parents: parents.iter().map(|parent| (*parent).clone()).collect(),
                device: device.into(),
                made_at,
                changes,
            };
            self.graph.insert(reference.id, commit);
            reference
        }
    }

    fn set(entry: &str, field: u32, value: Value) -> Change {
        Change::Set {
            entry: entry.into(),
            field,
            value,
        }
    }

    fn merged(outcome: MergeOutcome) -> (Commit, Entries, Vec<Conflict>) {
        match outcome {
            MergeOutcome::Merged {
                commit,
                entries,
                conflicts,
            } => (commit, entries, conflicts),
            MergeOutcome::FastForward(_) => panic!("expected a merge"),
        }
    }

    #[test]
    fn fields_changed_on_one_side_keep_that_sides_value() {
        let mut builder = Builder::new();
        let base = builder.commit(
            &[],
            "laptop",
            10,
            vec![
                set("dentist", 1, json!("Tuesday")),
                set("dentist", 2, json!("Dr. Ross")),
            ],
        );
        let laptop = builder.commit(
            &[&base],
            "laptop",
            20,
            vec![set("dentist", 1, json!("Friday"))],
        );
        let phone = builder.commit(
            &[&base],
            "phone",
            30,
            vec![set("dentist", 2, json!("Dr. Chen"))],
        );

        let (_, entries, conflicts) = merged(merge(&builder.graph, &laptop, &phone).unwrap());

        assert_eq!(entries["dentist"][&1], json!("Friday"));
        assert_eq!(entries["dentist"][&2], json!("Dr. Chen"));
        assert!(conflicts.is_empty());
    }

    #[test]
    fn a_field_both_sides_changed_takes_the_later_value_and_reports_the_other() {
        let mut builder = Builder::new();
        let base = builder.commit(&[], "laptop", 10, vec![set("dentist", 1, json!("Tuesday"))]);
        let laptop = builder.commit(
            &[&base],
            "laptop",
            40,
            vec![set("dentist", 1, json!("Friday"))],
        );
        let phone = builder.commit(
            &[&base],
            "phone",
            30,
            vec![set("dentist", 1, json!("Monday"))],
        );

        let (_, entries, conflicts) = merged(merge(&builder.graph, &phone, &laptop).unwrap());

        assert_eq!(entries["dentist"][&1], json!("Friday"));
        assert_eq!(
            conflicts,
            vec![Conflict {
                entry: "dentist".into(),
                field: 1,
                kept: Some(json!("Friday")),
                lost: Some(json!("Monday")),
            }]
        );
    }

    #[test]
    fn the_device_breaks_ties_at_the_same_second() {
        let mut builder = Builder::new();
        let base = builder.commit(&[], "laptop", 10, vec![set("a", 1, json!(0))]);
        let laptop = builder.commit(&[&base], "laptop", 30, vec![set("a", 1, json!(1))]);
        let phone = builder.commit(&[&base], "phone", 30, vec![set("a", 1, json!(2))]);

        let (_, entries, _) = merged(merge(&builder.graph, &laptop, &phone).unwrap());

        // "phone" sorts after "laptop".
        assert_eq!(entries["a"][&1], json!(2));
    }

    #[test]
    fn both_devices_write_the_same_merge_commit() {
        let mut builder = Builder::new();
        let base = builder.commit(&[], "laptop", 10, vec![set("a", 1, json!(0))]);
        let laptop = builder.commit(
            &[&base],
            "laptop",
            20,
            vec![set("a", 1, json!(1)), set("b", 1, json!("x"))],
        );
        let phone = builder.commit(&[&base], "phone", 30, vec![set("a", 1, json!(2))]);

        let (on_laptop, _, _) = merged(merge(&builder.graph, &laptop, &phone).unwrap());
        let (on_phone, _, _) = merged(merge(&builder.graph, &phone, &laptop).unwrap());

        assert_eq!(
            serde_json::to_vec(&on_laptop).unwrap(),
            serde_json::to_vec(&on_phone).unwrap()
        );
    }

    #[test]
    fn replaying_the_merge_commit_gives_the_merged_entries() {
        let mut builder = Builder::new();
        let base = builder.commit(
            &[],
            "laptop",
            10,
            vec![set("a", 1, json!(0)), set("b", 1, json!(0))],
        );
        let laptop = builder.commit(
            &[&base],
            "laptop",
            20,
            vec![set("a", 1, json!(1)), Change::Remove { entry: "b".into() }],
        );
        let phone = builder.commit(
            &[&base],
            "phone",
            30,
            vec![set("a", 1, json!(2)), set("c", 1, json!(3))],
        );

        let (commit, entries, _) = merged(merge(&builder.graph, &laptop, &phone).unwrap());
        let merge_ref = builder.commit(&[&laptop, &phone], "", commit.made_at, commit.changes);

        assert_eq!(builder.graph.entries_at(&merge_ref.id).unwrap(), entries);
        assert!(!entries.contains_key("b"));
        assert_eq!(entries["a"][&1], json!(2));
        assert_eq!(entries["c"][&1], json!(3));
    }

    #[test]
    fn a_head_that_follows_the_other_fast_forwards() {
        let mut builder = Builder::new();
        let base = builder.commit(&[], "laptop", 10, vec![set("a", 1, json!(0))]);
        let later = builder.commit(&[&base], "laptop", 20, vec![set("a", 1, json!(1))]);

        let outcome = merge(&builder.graph, &base, &later).unwrap();

        assert!(matches!(outcome, MergeOutcome::FastForward(head) if head.id == later.id));
    }
}
