# Rubbish - Sort of like Chubby, but in Rust .. Rubbish. 

Rubbish is a lock service and low-volume storage service with strong
consistency guarantees, designed to provide the basis for loosely-coupled
distributed systems.

It's also a chance to get some practice writing a full-sized application in
Rust.

The components of the system are as follows:

 * `cas` -- a distributed, content-addressible, in-memory storage system.  The
   system uses a "gossip"-style protocol to ensure that all participants have
   all content, and supports generational garbage collection and persistence
   to disk.

 * `treeish` -- a Git-like versioned filesystem, based on `cas`.  This includes
   the idea of a "commit" with parent commits and a nested tree structure
   associated with each commit.  It does not attempt to store a deep history
   for the filesystem, instead garbage collecting commits beyond configured
   thresholds even if they are part of the active history.

 * `prax` -- a distributed consensus service, designed to vote on the current
   head of the treeish commit tree and to define the current "master" node.
 
 * `rubfs` -- a filesystem-like layer on top of treeish and prax, supporting
   remote client operations such as read, write, check-and-swap, and advisory
   locks.  The provided client interface is HTTP.

The entire application is, of course, a work in progress.

## TODO

 * Support writing to trees (generates a new tree)
     * path copying - https://github.com/reem/adamantium/blob/master/src/list.rs
 * Use arrays and slices instead of vectors? For byte sequences?
