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
	"log"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "labrpc"

// import "bytes"
// import "../labgob"

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

type Log struct {
	term    int         // the term for this log entry
	command interface{} // empty interface for the actual command of this log entry
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

func (rf *Raft) PrintLog() {
	log.Printf("Raft server %d reports\nCurrentTerm: %d, voteFor: %d, commitIndex: %d, lastApplied: %d, role: %d, electionLastTime: %d, electionExpired: %t\n\n\n",
		rf.me, rf.currentTerm, rf.voteFor, rf.commitIndex, rf.lastApplied, rf.role, rf.electionLastTime.Unix(), rf.electionExpired)
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
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate who is requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm of the receiver, for the sender to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm { // my term is too old
		rf.currentTerm = args.Term // update my term first
		rf.voteFor = -1            // in this new term, I didn't vote for anyone
		rf.role = FOLLOWER         // convert myself to a follower, no matter what is my old role
	}

	if args.Term < rf.currentTerm { // my term is newer
		reply.Term = rf.currentTerm // return my current term to update the sender
		reply.VoteGranted = false   // the sender is too old, I don't vote for him
		return
	}

	if rf.voteFor == -1 || rf.voteFor == args.CandidateId { // either I vote for nobody, or I vote for you already
		// TODO: check whether candidate's log is AT LEAST as UP-TO_DATE as my log, if so, grant vote
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
// we don't use this one
//
//func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
//	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
//	return ok
//}

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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
	rf.logs = make([]Log, 0) // // initialized to my logs to a empty array slice

	// volatile state on all servers
	rf.commitIndex = 0 // initialized to 0, increases monotonically
	rf.lastApplied = 0 // initialized to 0, increases monotonically

	// volatile state on leaders (must be reinitialized after election)
	rf.nextIndex = make([]int, len(peers))  // don't need to initialize now since I'm not a leader on first boot
	rf.matchIndex = make([]int, len(peers)) // don't need to initialize now since I'm not a leader on first boot

	// initialized extra attributes I added
	rf.role = FOLLOWER // all servers should be followers when starting up
	rf.ResetTimer()    // start the timer

	go rf.SetTimer()    // set a timer to calculate elapsed time for election
	go rf.SetApplier()  // set a applier to apply logs (send through applyCh)
	go rf.MainRoutine() // start raft instance's main routine

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
			return
		}
		rf.mu.Lock()
		rf.PrintLog()
		switch rf.role {
		case LEADER: // if you are a leader, then you should send heartbeats
			rf.mu.Unlock() // release the lock now since to send heartBeat we will require the lock again
			// TODO: send heartbeat via AppendEntries RPC
			time.Sleep(10 * time.Millisecond) // after sleep a while, you need to send heartbeat again
			break
		case CANDIDATE: // if you are a candidate, you should start a election
			rf.currentTerm++   // increment my current term
			rf.voteFor = rf.me // vote for myself
			rf.mu.Unlock()     // release the lock now since RunElection requires its own lock
			rf.RunElection()   // send requestVote RPCs to all other servers
			break
		case FOLLOWER: // if you are a follower, you should do nothing but wait for RPC from your leader
			if rf.electionExpired { // but first check election timeout
				rf.role = CANDIDATE // if it expires, then convert to candidate and proceed
				rf.ResetTimer()     // restart the election timer because I'm starting an election
				continue
			}
			rf.mu.Unlock()                    // release the lock now since BecomeCandidate is not thread safe
			time.Sleep(10 * time.Millisecond) // after sleep a while, and check if my timeout expires again
			break                             // if no timeout then it is fine, RPC call is handled in its handler so nothing to do here
		}
	}
}

//
// as a raft instance becomes a candidate, it asks all its peers to vote after it votes for itself
// it send RPC request for each server in a separate goroutine and handle RPC reply in a same goroutine;
// according to the result of the election, it decides whether it becomes the leader or convert to follower;
// since labrpc guarantees that every RPC returns, we don't need to handle RPC timeout, etc.
// This function quits only when raft is no longer candidate, or due to timeout it needs to re-elect
//
func (rf *Raft) RunElection() {
	rf.mu.Lock()
	term := rf.currentTerm                    // my term
	candidateId := rf.me                      // candidate id, which is me!
	lastLogIndex := len(rf.logs) - 1          // index of my last log entry
	lastLogTerm := rf.logs[lastLogIndex].term // term of my last log entry
	rf.mu.Unlock()

	electionDone := sync.NewCond(new(sync.Mutex))
	upVote := 1   // how many servers agree to vote for me (initialized to 1 since I vote for myself!)
	downVote := 0 // how many servers disagree to vote for me

	for idx, _ := range rf.peers {
		if idx == candidateId {
			continue // I don't have to vote for myself, I did this in my MainRoutine function
		}
		args := RequestVoteArgs{term, candidateId, lastLogIndex, lastLogTerm}
		reply := RequestVoteReply{}
		go rf.sendRequestVote(idx, &args, &reply, &upVote, &downVote) // send RPC to each server
	}

	go func(rf *Raft) { // once I am no longer a candidate or election time expires, I will immediately quit this function
		for { // since ultimately there will be election timeout, so this goroutine eventually quits
			rf.mu.Lock()                                    // lock raft instance to access its role
			if rf.role != CANDIDATE || rf.electionExpired { // check if I'm still a candidate and election timeout
				electionDone.Signal() // notify the function that the state is changed
				rf.mu.Unlock()        // since you will return in this if section, release the lock now
				return                // time to bail!
			}
			rf.mu.Unlock()
			time.Sleep(15 * time.Millisecond) // sleep a while to save CPU and check role later
		}
	}(rf)

	electionDone.Wait() // wait for the election checker goroutine above
}

//
// send RequestVote RPC to a single server and handle the reply
// also signal the voteDone cond var
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, upVote *int, downVote *int) {

	rf.peers[server].Call("Raft.RequestVote", args, reply)

	// TODO: do we really have to discard reply with a different term that mismatches args' term?

	rf.mu.Lock()         // add mutex lock before you access attributes of raft instance
	defer rf.mu.Unlock() // release mutex lock when the function quits

	if rf.role != CANDIDATE { // no need to count the vote since I'm no longer a candidate
		return
	}

	if reply.Term < rf.currentTerm { // this RPC is too old (a newer term now), just discard it
		return
	}

	if reply.Term > rf.currentTerm { // someone's term is bigger than me, so I'm not the newest one
		rf.currentTerm = reply.Term // update my current term
		rf.role = FOLLOWER          // convert to follower
		return
	}

	if reply.VoteGranted { // this server agree to vote for me
		*upVote++
		if *upVote > len(rf.peers)/2 { // I won majority
			rf.role = LEADER // so I'm a leader now
		}
	} else { // this server agree to vote for me
		*downVote++        // I lost majority
		rf.role = FOLLOWER // so I'm a follower now
	}
}

//
// the applier can be awaken by a cond var; every time there are logs are committed,
// you should awake the cond var and tell this function to apply logs
// which is to send log through applyCh, and also increase raft instance's lastApplied value
//
func (rf *Raft) SetApplier() {
	for {
		if rf.killed() { // if the raft instance is killed, it means this test is finished and we should quit
			return
		}
		// TODO: finish this shit when you do 2(B), I left it alone since it's no use for 2(A)!
	}
}

//
// the timer to calculate time passed since rf.electionLastTime
// if the time is more than a random threshold between 150ms to 300ms
// then there is a timeout, and the raft instance should set electionExpired to true
// to notify itself it should run a new election
//
func (rf *Raft) SetTimer() {
	rand.Seed(time.Now().Unix())    // set a random number seed to ensure it generates different random number
	timeout := rand.Int()%150 + 150 // generate a random timeout threshold between 150 to 300ms
	for {
		if rf.killed() { // if the raft instance is killed, it means this test is finished and we should quit
			return
		}
		time.Sleep(10 * time.Millisecond) // sleep a while to save some CPU time
		electionCurrentTime := time.Now()
		rf.mu.Lock()
		log.Printf("Raft server %d reports\nCurrent time: %d, electionLasttime: %d\n\n", rf.me, electionCurrentTime.Unix(), rf.electionLastTime.Unix())
		if electionCurrentTime.Unix()-rf.electionLastTime.Unix() > int64(timeout) { // timeout!
			rf.electionExpired = true                 // election time expired! you should run a new election now
			rf.electionLastTime = electionCurrentTime // reset the timer
			timeout = rand.Int()%150 + 150            // reset the random timeout threshold
		}
		rf.mu.Unlock()
	}
}

//
// reset timer
//
func (rf *Raft) ResetTimer() {
	rf.electionExpired = false
	rf.electionLastTime = time.Now()
}
