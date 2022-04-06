package raft

// CondInstallSnapshot
//
// A service wants to switch to snapshot.  Only do so if Raft doesn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot
//
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
//
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.LogSnapshotIn(index)
	defer rf.LogSnapshotOut(index)

	if index <= rf.lastIncludedIndex {
		return
	}

	rf.snapshot = snapshot

	// log         : 0 1 2 3 4 5 6 7 8
	// ActualIndex:              0 1 2
	// Snapshot:     |---------|
	// index:                      |
	// index = 7, lastIncludedIndex = 5, so actualIndex should be index-lastIncludedIndex-1
	actualIndex := index - rf.lastIncludedIndex - 1
	rf.lastIncludeTerm = rf.logs[actualIndex].Term

	// update lastIncludedIndex and lastIncludeTerm
	rf.lastIncludedIndex = index

	// trim my log
	rf.logs = rf.logs[actualIndex+1:]
	// Use a new slice so the old array can be released
	logCopy := make([]Log, len(rf.logs))
	copy(logCopy, rf.logs)
	rf.logs = logCopy

	// persist
	rf.persist(snapshot)

	if rf.role == LEADER {
		go rf.RequestReplication(rf.currentTerm)
	}
}
