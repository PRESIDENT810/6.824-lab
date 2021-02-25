package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"
	"time"
)
import "sync/atomic"
import "labrpc"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// defined const to identify roles for each server
const (
	LEADER    = 0
	CANDIDATE = 1
	FOLLOWER  = 2
)

// whether show corresponding log
const (
	showLog     = false
	showLock    = false
	showPersist = false
)

// if raft instance is supposed to save its state, pass a integer to this channel
var stateChange chan int

type Log struct {
	Term    int         // the term for this log entry
	Command interface{} // empty interface for the actual command of this log entry
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// states listed in figure 2
	currentTerm int   // what I think my current term is, persistent state
	voteFor     int   // who I vote for in my current term, persistent state
	logs        []Log // what is my logs, persistent state

	commitIndex int // index of the last committed log, volatile state
	lastApplied int // index of the last applied log, volatile state

	nextIndex  []int // indices of the next log I should send to each follower, volatile state for leader only
	matchIndex []int // indices of the last log that matches to each follower, volatile state for leader only

	// extra attributes I might need later
	role             int       // 0 for LEADER; 1 for CANDIDATE; 2 for FOLLOWER
	electionLastTime time.Time // timestamp when last time heard from a leader
	electionExpired  bool      // if true, then last election expired and should run new election
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm        // give my current term
	isleader = rf.role == LEADER // true if I am the LEADER
	rf.mu.Unlock()
	return term, isleader
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.logs)
	term := rf.currentTerm
	isLeader := rf.role == LEADER

	Printf("\n====================New log %d from client to server %d with term %d, isLeader=%t====================\n", command, rf.me, term, isLeader)

	if isLeader {
		rf.logs = append(rf.logs, Log{rf.currentTerm, command}) // log to replicate to the cluster
		go rf.persist()                                         // logs are changed, so I need to save my states
		go rf.RequestReplication(rf.currentTerm)
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// persistent state on all servers
	rf.currentTerm = 0       // initialized to 0 on first boot, increases monotonically
	rf.voteFor = -1          // initialized to -1 to indicate I vote for no one at beginning
	rf.logs = make([]Log, 1) // // log array slice starts with 1!

	// volatile state on all servers
	rf.commitIndex = 0 // initialized to 0, increases monotonically
	rf.lastApplied = 0 // initialized to 0, increases monotonically

	// volatile state on leaders (must be reinitialized after election)
	rf.nextIndex = make([]int, len(peers)) // don't need to initialize now since I'm not a leader on first boot
	for idx, _ := range rf.nextIndex {
		rf.nextIndex[idx] = 1 // log starts with 1, so the nextIndex should be 1 (every raft peer has the same log[0]!)
	}
	rf.matchIndex = make([]int, len(peers)) // don't need to initialize now since I'm not a leader on first boot

	// initialized extra attributes I added
	rf.role = FOLLOWER // all servers should be followers when starting up
	rf.ResetTimer()    // initialize the timer

	go rf.SetTimer()          // set a timer to calculate elapsed time for election
	go rf.SetApplier(applyCh) // set a applier to apply logs (send through applyCh)
	go rf.MainRoutine()       // start raft instance's main routine

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

//
// the main raft routine periodically checks its state
// if raft is a follower or candidate (not sure about this) with election timeout,
// then it runs a election trying to become a leader
// if raft is a leader, then send heartbeats to its followers
//
func (rf *Raft) MainRoutine() {
	for {
		if rf.killed() { // if the raft instance is killed, it means this test is finished and we should quit
			stateChange <- 0 // awaken the saveState function to let it know it's time to quit
			return
		}
		rf.mu.Lock()
		switch rf.role {
		case LEADER: // if you are a leader, then you should send heartbeats
			//Printf("\n=====================================================================================================\n")
			//Printf("=================================[Server%d] is LEADER=================================\n", rf.me)
			//Printf("=====================================================================================================\n")
			go rf.SendHeartbeats(rf.currentTerm) // block here to ensure that no more than 10 heartbeat being sent in a second
			rf.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			break
		case CANDIDATE: // if you are a candidate, you should start a election
			//Printf("\n=====================================================================================================\n")
			//Printf("=================================[Server%d] is CANDIDATE=================================\n", rf.me)
			//Printf("=====================================================================================================\n")
			if rf.electionExpired {
				rf.currentTerm++                  // increment my current term
				rf.voteFor = rf.me                // vote for myself
				rf.electionExpired = false        // reset electionExpired to let timer decided when to re-elect
				go rf.persist()                   // currentTerm and voteFor are changed, so I need to save my states
				go rf.RunElection(rf.currentTerm) // if electionExpired is false, it means you elect too fast, wait for the timeout
			}
			rf.mu.Unlock()
			time.Sleep(20 * time.Millisecond)
			break
		case FOLLOWER: // if you are a follower, you should do nothing but wait for RPC from your leader
			//Printf("\n=====================================================================================================\n")
			//Printf("=================================[Server%d] is FOLLOWER=================================\n", rf.me)
			//Printf("=====================================================================================================\n")
			if rf.electionExpired { // but first check election timeout
				rf.ResetTimer()     // restart election timer if I am starting an election
				rf.role = CANDIDATE // if it expires, then convert to candidate and proceed
			}
			rf.mu.Unlock()
			time.Sleep(20 * time.Millisecond) // after sleep a while, and check if my timeout expires again
			break                             // if no timeout then it is fine, RPC call is handled in its handler so nothing to do here
		}
	}
}
