use crate::{Index, Term};
use failure::{err_msg, Fallible};
use serde::{Deserialize, Serialize};
use std::ops::{Bound::*, RangeBounds};

/// A LogEntry is an entry in a RaftLog.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogEntry<I: Clone> {
    pub term: Term,
    pub item: I,
}

/// A RaftLog represents the central data structure in raft, and enforces correct access to that
/// log.  Note that indexes are 1-based!
#[derive(Clone, Debug, PartialEq)]
pub struct RaftLog<I: Clone> {
    entries: Vec<LogEntry<I>>,
}

impl<I: Clone> LogEntry<I> {
    pub fn new(term: Term, item: I) -> LogEntry<I> {
        LogEntry { term, item }
    }
}

impl<I: Clone> RaftLog<I> {
    /// Create an empty log
    pub fn new() -> RaftLog<I> {
        RaftLog { entries: vec![] }
    }

    /// Get the number of entries
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Get the index of the last entry in the log
    pub fn last_index(&self) -> Option<Index> {
        let len = self.entries.len();
        if len > 0 {
            Some(len as Index)
        } else {
            None
        }
    }

    /// Get the index of the next entry to be added to the log
    pub fn next_index(&self) -> Index {
        self.entries.len() as Index + 1
    }

    /// Get an entry by its index
    pub fn get(&self, index: Index) -> &LogEntry<I> {
        assert!(index >= 1, "logs have no index 0");
        &self.entries[index as usize - 1]
    }

    /// Slice the log entries by index
    pub fn slice<R: RangeBounds<usize>>(&self, range: R) -> &[LogEntry<I>] {
        let start_vec_index = match range.start_bound() {
            Included(i) => i - 1,
            Excluded(i) => *i,
            Unbounded => 0,
        };
        let end_vec_index = match range.end_bound() {
            Included(i) => *i,
            Excluded(i) => i - 1,
            Unbounded => self.entries.len(),
        };
        &self.entries[start_vec_index..end_vec_index]
    }

    /// Add entry (used in testing)
    #[cfg(test)]
    pub fn add(&mut self, entry: LogEntry<I>) {
        self.entries.push(entry);
    }

    /// Append entries to the log, applying the necessary rules from the Raft protocol and
    /// returning false if those fail
    pub fn append_entries<E: AsRef<[LogEntry<I>]>>(
        &mut self,
        prev_index: Index,
        prev_term: Term,
        entries: E,
    ) -> Fallible<()> {
        let entries = entries.as_ref();
        let len = self.len();

        // Rule 1: no gaps in the log indexes
        if prev_index > len as u64 {
            return Err(err_msg(format!(
                "prev_index {} is not present in this log",
                prev_index
            )));
        }

        // Rule 2/3: prev_term
        if prev_index >= 1 {
            let prev = &self.get(prev_index);
            if prev.term != prev_term {
                return Err(err_msg(format!(
                    "Entry at index {} has term {} but expected term {}",
                    prev_index, prev.term, prev_term
                )));
            }
        }

        // insert the entries, replacing any at the given index and above
        self.entries.truncate(prev_index as usize);
        self.entries.reserve(prev_index as usize + entries.len());
        for entry in entries.iter() {
            self.entries.push((*entry).clone());
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    type Item = char;

    #[test]
    fn len_zero() {
        let log: RaftLog<Item> = RaftLog::new();
        assert_eq!(log.len(), 0);
    }

    #[test]
    fn last_index_zero() {
        let log: RaftLog<Item> = RaftLog::new();
        assert_eq!(log.last_index(), None);
    }

    #[test]
    fn next_index_zero() {
        let log: RaftLog<Item> = RaftLog::new();
        assert_eq!(log.next_index(), 1);
    }

    #[test]
    fn len_nonzero() -> Fallible<()> {
        let mut log: RaftLog<Item> = RaftLog::new();
        log.add(LogEntry::new(0, 'a'));
        log.add(LogEntry::new(0, 'b'));
        assert_eq!(log.len(), 2);
        Ok(())
    }

    #[test]
    fn last_index_nonzero() -> Fallible<()> {
        let mut log: RaftLog<Item> = RaftLog::new();
        log.add(LogEntry::new(0, 'a'));
        log.add(LogEntry::new(0, 'b'));
        assert_eq!(log.last_index(), Some(2));
        Ok(())
    }

    #[test]
    fn net_index_nonzero() -> Fallible<()> {
        let mut log: RaftLog<Item> = RaftLog::new();
        log.add(LogEntry::new(0, 'a'));
        log.add(LogEntry::new(0, 'b'));
        assert_eq!(log.next_index(), 3);
        Ok(())
    }

    #[test]
    fn get() -> Fallible<()> {
        let mut log: RaftLog<Item> = RaftLog::new();
        log.add(LogEntry::new(0, 'a'));
        log.add(LogEntry::new(0, 'b'));
        assert_eq!(log.get(1), &LogEntry::new(0, 'a'));
        assert_eq!(log.get(2), &LogEntry::new(0, 'b'));
        Ok(())
    }

    #[test]
    fn slice() -> Fallible<()> {
        let mut log: RaftLog<Item> = RaftLog::new();
        let a = LogEntry::new(100, 'a');
        log.add(a.clone());
        let b = LogEntry::new(100, 'b');
        log.add(b.clone());
        let c = LogEntry::new(100, 'c');
        log.add(c.clone());
        let d = LogEntry::new(100, 'd');
        log.add(d.clone());
        assert_eq!(log.slice(..), &[a.clone(), b.clone(), c.clone(), d.clone()]);
        assert_eq!(log.slice(1..2), &[a.clone()]);
        assert_eq!(log.slice(1..=2), &[a.clone(), b.clone()]);
        assert_eq!(log.slice(..3), &[a.clone(), b.clone()]);
        assert_eq!(log.slice(3..), &[c.clone(), d.clone()]);
        Ok(())
    }

    #[test]
    fn append_with_gaps() -> Fallible<()> {
        let mut log: RaftLog<Item> = RaftLog::new();
        log.append_entries(0, 0, vec![LogEntry::new(0, 'a'), LogEntry::new(0, 'b')])?;
        assert!(log.append_entries(3, 0, vec![]).is_err());
        Ok(())
    }

    #[test]
    fn append_with_mismatched_prev_term() -> Fallible<()> {
        let mut log: RaftLog<Item> = RaftLog::new();
        log.append_entries(0, 0, vec![LogEntry::new(100, 'a'), LogEntry::new(200, 'b')])?;
        assert!(log.append_entries(1, 999, vec![]).is_err());
        Ok(())
    }

    #[test]
    fn append_entries_appends() -> Fallible<()> {
        let mut log: RaftLog<Item> = RaftLog::new();
        log.append_entries(0, 0, vec![LogEntry::new(100, 'a'), LogEntry::new(200, 'b')])?;
        log.append_entries(
            2,
            200,
            vec![LogEntry::new(300, 'c'), LogEntry::new(300, 'd')],
        )?;
        assert_eq!(log.get(1), &LogEntry::new(100, 'a'));
        assert_eq!(log.get(2), &LogEntry::new(200, 'b'));
        assert_eq!(log.get(3), &LogEntry::new(300, 'c'));
        assert_eq!(log.get(4), &LogEntry::new(300, 'd'));
        Ok(())
    }

    #[test]
    fn append_entries_overwrites() -> Fallible<()> {
        let mut log: RaftLog<Item> = RaftLog::new();
        log.append_entries(0, 0, vec![LogEntry::new(100, 'a'), LogEntry::new(200, 'b')])?;
        log.append_entries(
            1,
            100,
            vec![LogEntry::new(300, 'c'), LogEntry::new(300, 'd')],
        )?;
        assert_eq!(log.get(1), &LogEntry::new(100, 'a'));
        assert_eq!(log.get(2), &LogEntry::new(300, 'c'));
        assert_eq!(log.get(3), &LogEntry::new(300, 'd'));
        Ok(())
    }

    #[test]
    fn append_entries_idempotent() -> Fallible<()> {
        let mut log: RaftLog<Item> = RaftLog::new();
        log.append_entries(0, 0, vec![LogEntry::new(100, 'a'), LogEntry::new(200, 'b')])?;

        log.append_entries(
            1,
            100,
            vec![LogEntry::new(300, 'c'), LogEntry::new(300, 'd')],
        )?;
        let orig = log.clone();
        log.append_entries(
            1,
            100,
            vec![LogEntry::new(300, 'c'), LogEntry::new(300, 'd')],
        )?;
        log.append_entries(
            1,
            100,
            vec![LogEntry::new(300, 'c'), LogEntry::new(300, 'd')],
        )?;

        assert_eq!(orig, log);
        Ok(())
    }

    // test cases from Figure 7

    fn case(terms: Vec<Term>) -> RaftLog<Item> {
        let mut log: RaftLog<Item> = RaftLog::new();
        log.append_entries(
            0,
            0,
            terms
                .iter()
                .map(|t| LogEntry::new(*t, 'x'))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        log
    }

    #[test]
    fn figure_7_a() -> Fallible<()> {
        let mut log = case(vec![1, 1, 1, 4, 4, 5, 5, 6, 6]);
        assert!(log
            .append_entries(10, 6, vec![LogEntry::new(8, 'x')])
            .is_err());
        Ok(())
    }

    #[test]
    fn figure_7_b() -> Fallible<()> {
        let mut log = case(vec![1, 1, 1, 4]);
        assert!(log
            .append_entries(10, 6, vec![LogEntry::new(8, 'x')])
            .is_err());
        Ok(())
    }

    #[test]
    fn figure_7_c() -> Fallible<()> {
        let mut log = case(vec![1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6]);
        log.append_entries(10, 6, vec![LogEntry::new(8, 'x')])?;
        assert_eq!(log.get(11), &LogEntry::new(8, 'x'));
        Ok(())
    }

    #[test]
    fn figure_7_d() -> Fallible<()> {
        let mut log = case(vec![1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7]);
        log.append_entries(10, 6, vec![LogEntry::new(8, 'x')])?;
        assert_eq!(log.get(11), &LogEntry::new(8, 'x'));
        assert_eq!(log.len(), 11);
        Ok(())
    }

    #[test]
    fn figure_7_e() -> Fallible<()> {
        let mut log = case(vec![1, 1, 1, 4, 4, 4, 4]);
        assert!(log
            .append_entries(10, 6, vec![LogEntry::new(8, 'x')])
            .is_err());
        Ok(())
    }

    #[test]
    fn figure_7_f() -> Fallible<()> {
        let mut log = case(vec![1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3]);
        assert!(log
            .append_entries(10, 6, vec![LogEntry::new(8, 'x')])
            .is_err());
        Ok(())
    }
}
