use crate::{Index, Term};
use failure::{err_msg, Fallible};
use serde::{Deserialize, Serialize};

/// A LogEntry is an entry in a RaftLog.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogEntry<I> {
    pub term: Term,
    pub item: I,
}

/// A RaftLog represents the central data structure in raft, and enforces correct access to that
/// log.
#[derive(Clone, Debug, PartialEq)]
pub struct RaftLog<I> {
    entries: Vec<LogEntry<I>>,
}

impl<I> LogEntry<I> {
    pub fn new(term: Term, item: I) -> LogEntry<I> {
        LogEntry { term, item }
    }
}

impl<I> RaftLog<I> {
    /// Create an empty log
    pub fn new() -> RaftLog<I> {
        RaftLog { entries: vec![] }
    }

    /// Get the number of entries
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Get an entry by its index
    pub fn get(&self, index: Index) -> &LogEntry<I> {
        &self.entries[index as usize]
    }

    /// Append entries to the log, applying the necessary rules from the Raft protocol and
    /// returning false if those fail
    pub fn append_entries(
        &mut self,
        index: Index,
        prev_term: Term,
        entries: Vec<LogEntry<I>>,
    ) -> Fallible<()> {
        let len = self.len();

        // Rule 1: no gaps in the log indexes
        if index > len as u64 {
            return Err(err_msg(format!(
                "Index {} is higher than the next index {}",
                index, len
            )));
        }

        // Rule 2/3: prev_term
        if index > 0 {
            let last = &self.entries[index as usize - 1];
            if last.term != prev_term {
                return Err(err_msg(format!(
                    "Entry at index {} has term {} but expected term {}",
                    index - 1,
                    last.term,
                    prev_term
                )));
            }
        }

        // insert the entries, replacing any at the given index and above
        self.entries.splice((index as usize).., entries);

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
    fn len_nonzero() -> Fallible<()> {
        let mut log: RaftLog<Item> = RaftLog::new();
        log.append_entries(0, 0, vec![LogEntry::new(0, 'a'), LogEntry::new(0, 'b')])?;
        assert_eq!(log.len(), 2);
        Ok(())
    }

    #[test]
    fn get() -> Fallible<()> {
        let mut log: RaftLog<Item> = RaftLog::new();
        log.append_entries(0, 0, vec![LogEntry::new(0, 'a'), LogEntry::new(0, 'b')])?;
        assert_eq!(log.get(0), &LogEntry::new(0, 'a'));
        assert_eq!(log.get(1), &LogEntry::new(0, 'b'));
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
        assert_eq!(log.get(0), &LogEntry::new(100, 'a'));
        assert_eq!(log.get(1), &LogEntry::new(200, 'b'));
        assert_eq!(log.get(2), &LogEntry::new(300, 'c'));
        assert_eq!(log.get(3), &LogEntry::new(300, 'd'));
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
        assert_eq!(log.get(0), &LogEntry::new(100, 'a'));
        assert_eq!(log.get(1), &LogEntry::new(300, 'c'));
        assert_eq!(log.get(2), &LogEntry::new(300, 'd'));
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
        log.append_entries(0, 0, terms.iter().map(|t| LogEntry::new(*t, 'x')).collect())
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
        assert_eq!(log.get(10), &LogEntry::new(8, 'x'));
        Ok(())
    }

    #[test]
    fn figure_7_d() -> Fallible<()> {
        let mut log = case(vec![1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7]);
        log.append_entries(10, 6, vec![LogEntry::new(8, 'x')])?;
        assert_eq!(log.get(10), &LogEntry::new(8, 'x'));
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
