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

 * `fs` -- a Git-like versioned filesystem, based on `cas`.  This includes
   the idea of a "commit" with parent commits and a nested tree structure
   associated with each commit. 

 * `prax` -- a distributed consensus service, designed to vote on the current
   head of the treeish commit tree and to define the current "master" node.
 
 * `tip` -- a layer on top of treeish and prax, supporting
   remote client operations such as read, write, check-and-swap, and advisory
   locks.  The provided client interface is HTTP.

The entire application is, of course, a work in progress.

## TODO

### Fs Refactor

* Write modification methods for Tree
* Write wrappers for them on Commit (see old code)
* Docs
* Module-level example tests

### Single-Hosted

Finish this application in a non-networked state

* Implement tip API
* Implement generational garbage collection between cas and fs

### Misc

* use pub(crate) (rust 1.19?)
* use serde, or better yet some write-based thing so we don't depend on repeatability of hash ordering

### Distribute

* Build a network interface
* Teach cas to gossip
* Teach prax to reach consensus
* Build a server binary

### Practicality

* Teach cas to journal to disk (use GC generations to control file size)
* Logging
* Write a simple command-line client
* Access control

### Rename Things

* pile
* heap
* dumpster
* truck

### Misc

* Travis
* Enforce rustfmt
* Run coverage
* Rustish docs
