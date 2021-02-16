package raft

//
// A server calls Snapshot() to communicate the snapshot of its state to Raft.
// The snapshot includes all info up to and including index.
// This means the server no longer needs the log through (and including) index.
//
func Snapshot(index int, snapshot []byte) {

}

func CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}
