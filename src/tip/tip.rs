use tip::error::*;
use cas::{Hash, CAS};
use fs::{FileSystem, FS};
use prax::{Prax, DistributedState};

/// The current state of the rubbish cluster, represented by the hash of the latest commit
/// in the FS.
type State = Hash;

/// A Change is represented as an old and new hash. If applied to a State that is not currently at
/// the old hash, the change is not applied.
pub struct Change {
    // TODO: why does this have to be pub (warning otherwise)
    old: Hash,
    new: Hash,
}

impl DistributedState for State {
    type Change = Change;

    fn update(&self, change: Change) -> Option<State> {
        if self == &change.old {
            Some(change.new)
        } else {
            None
        }
    }
}

pub struct Tip<'a, C: 'a + CAS> {
    storage: &'a C,
    fs: FileSystem<'a, C>,
    prax: Prax<State>,
}

impl<'a, C> Tip<'a, C>
    where C: 'a + CAS
{
    pub fn new(storage: &'a C) -> Result<Tip<'a, C>> {
        let fs = FileSystem::new(storage);
        let root = fs.root_commit();
        let root_hash = root.store(&storage)?;
        let prax = Prax::new(root_hash);

        Ok(Tip {
               storage: storage,
               fs: fs,
               prax: prax,
           })
    }

    /// Read a value from the cluster, returning the current value as of the beginning of this
    /// call.
    pub fn read(&self, path: &[&str]) -> Result<String> {
        let state = self.prax.read();
        let commit = self.fs.get_commit(state)?;
        let tree = commit.tree();
        Ok(tree.read(self.storage, path)?)
    }

    /*
    /// Write a value to the cluster unconditionally.
    pub fn write(&self, path: &[&str], value: String) -> Result<()> {
        let state = self.prax.read();

        // TODO: make a helper loop to try an update until successful or aborted

        let parent = self.fs.get_commit(state)?;
        let child = parent.update().write(path, value).commit()?;

        let new_state = child.store(self.storage);
        if !self.prax
                .update(Change {
                            old: state,
                            new: new_state,
                        }) {
            bail!("conflicting update"); // TODO: don't bail!
        }

        Ok(())
    }
    */

    // TODO: test-and-set
}

#[cfg(test)]
mod test {
    use super::Tip;
    use cas::LocalStorage;

    #[test]
    fn test_read_missing_path() {
        let storage = LocalStorage::new();
        let tip = Tip::new(&storage).unwrap();

        assert!(tip.read(&["a", "b"]).is_err());
    }
}
