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
	"fmt"
	"log"
	"math/rand"
	"strconv"
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

// defined const to identify
const (
	SENDING = 0
	SUCCESS = 1
	FAIL    = 2
)

type Log struct {
	Term    int         // the term for this log entry
	Command interface{} // empty interface for the actual command of this log entry
}

func (log *Log) String() string {
	return strconv.Itoa(log.Term)
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

// TODO: add mutex for Log functions so their output won't mix up!
var logMutex sync.Mutex

//
// log server info
//
func (rf *Raft) LogServerStates() {
	fmt.Printf("=======================================================================================================\n")
	fmt.Printf("[server%d] reports\nCurrentTerm: %d, voteFor: %d, commitIndex: %d, lastApplied: %d, role: %d, electionExpired: %t\n",
		rf.me, rf.currentTerm, rf.voteFor, rf.commitIndex, rf.lastApplied, rf.role, rf.electionExpired)
	fmt.Printf("My log entries are: %v\n", rf.logs)
	fmt.Printf("Volatile state on leaders:\n")
	fmt.Printf("nextIndex: ")
	for _, i := range rf.nextIndex {
		fmt.Printf("%d ", i)
	}
	fmt.Printf("\nmatchIndex: ")
	for _, i := range rf.matchIndex {
		fmt.Printf("%d ", i)
	}
	fmt.Printf("\n")
	fmt.Printf("=======================================================================================================\n\n")
}

//
// log AppendEntries RPC info on receiver side
//
func (rf *Raft) LogAppendEntriesReceive(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	logMutex.Lock()
	defer logMutex.Unlock()
	fmt.Printf("[server%d] receives AppendEntries RPC\nArguments:\nterm: %d, leaderId: %d, prevLogIndex: %d, prevLogTerm: %d, entry: %v, leaderCommit: %d\n\n\n",
		rf.me, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)
	rf.LogServerStates()
}

//
// log RequestVote RPC info on sender side
//
func (rf *Raft) LogRequestVoteReceive(args *RequestVoteArgs, reply *RequestVoteReply) {
	logMutex.Lock()
	defer logMutex.Unlock()
	fmt.Printf("[server%d] receives RequestVote RPC\nArguments:\nterm: %d, candidateId: %d, lastLogIndex: %d, lastLogTerm: %d\n\n\n",
		rf.me, args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
	rf.LogServerStates()
}

//
// log AppendEntries RPC info on sender side
//
func (rf *Raft) LogAppendEntriesSend(sender, receiver int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	logMutex.Lock()
	defer logMutex.Unlock()
	fmt.Printf("[server%d] sends AppendEntries RPC to %d\nArguments:\nterm: %d, leaderId: %d, prevLogIndex: %d, prevLogTerm: %d, entry: %v, leaderCommit: %d\nReply:\nterm: %d, success: %t\n\n\n",
		sender, receiver, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit, reply.Term, reply.Success)
	rf.LogServerStates()
}

//
// log RequestVote RPC info on sender side
//
func (rf *Raft) LogRequestVoteSend(sender, receiver int, args *RequestVoteArgs, reply *RequestVoteReply) {
	logMutex.Lock()
	defer logMutex.Unlock()
	fmt.Printf("[server%d] sends RequestVote RPC to %d\nArguments:\nterm: %d, candidateId: %d, lastLogIndex: %d, lastLogTerm: %d\nReply:\nterm: %d, voteGranted: %t\n\n\n",
		sender, receiver, args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm, reply.Term, reply.VoteGranted)
	rf.LogServerStates()
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
//f
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
// RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate who is requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int  // currentTerm of the receiver, for the sender to update itself
	VoteGranted bool // true means candidate received vote
}

//
// AppendEntries RPC arguments structure
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term         int   // leader's term
	LeaderId     int   // to let receiver know who is the current leader
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log // log entries to store (empty for heartbeat; includes more than one for efficiency)
	LeaderCommit int   // leader's commitIndex
}

//
// AppendEntries RPC reply structure
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

//
// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	rf.LogAppendEntriesReceive(args, reply)
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm { // my term is newer
		reply.Term = rf.currentTerm // return my current term to update the sender
		reply.Success = false       // reply false if term < currentTerm
		return
	}

	rf.ResetTimer() // reset timer upon AppendEntries (but if the term in arguments is outdated, you should not reset your timer!)

	if args.Term > rf.currentTerm { // my term is too old
		rf.currentTerm = args.Term // update my term first
		rf.voteFor = -1            // in this new term, I didn't vote for anyone
		rf.role = FOLLOWER         // convert myself to a follower, no matter what is my old role
	}

	prevLogIndex := args.PrevLogIndex // use a local variable to store prevLogIndex for efficiency

	if len(rf.logs) <= prevLogIndex || rf.logs[prevLogIndex].Term != args.PrevLogTerm { // our log mismatch (either I don't have this log or have a log with different term)
		reply.Term = rf.currentTerm // same as sender's term (if newer, I returned false already; if older, I updated already)
		reply.Success = false       // reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
		return
	}

	// if an existing entry conflicts with a new one (args.entries), delete the existing entry and all that follow it
	// append any new entries not already in the log
	for i, _ := range args.Entries {
		insertIndex := i + prevLogIndex + 1 // i+prevLogIndex <= last index of log (if len(log) <= prevLogIndex, it returns already)
		if insertIndex == len(rf.logs) {    // no log at insertIndex, so we append a new entry
			rf.logs = append(rf.logs, args.Entries[i])
		} else if rf.logs[insertIndex] != args.Entries[i] { // log at insertIndex doesn't match our entry
			rf.logs = rf.logs[:insertIndex]            // delete the existing entry and all that follow it
			rf.logs = append(rf.logs, args.Entries[i]) // add current log
		} else { // log at insertIndex matches our entry
			continue
		}
	}

	if args.LeaderCommit > rf.commitIndex { // rule 5 for AppendEntries RPC in figure 2
		rf.commitIndex = min(args.LeaderCommit, prevLogIndex+len(args.Entries))
	}
	reply.Term = rf.currentTerm
	reply.Success = true
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	rf.LogRequestVoteReceive(args, reply)
	defer rf.mu.Unlock()

	upToDate := true // is your log more up-to-date?
	myLastLogIndex := len(rf.logs) - 1
	if args.LastLogTerm != rf.logs[myLastLogIndex].Term { // we have different terms
		upToDate = args.LastLogTerm > rf.logs[myLastLogIndex].Term // you are up-to-date if your term is larger
	} else { // we have same terms
		upToDate = args.LastLogIndex >= myLastLogIndex // you are up-to-date if you log is longer
	}

	if args.Term > rf.currentTerm { // my term is too old
		rf.currentTerm = args.Term // update my term first
		rf.voteFor = -1            // in this new term, I didn't vote for anyone
		rf.role = FOLLOWER         // convert myself to a follower, no matter what is my old role
		// remember when receiving RequestVote RPC you shouldn't reset the timer!!
	}

	if args.Term < rf.currentTerm { // my term is newer
		reply.Term = rf.currentTerm // return my current term to update the sender
		reply.VoteGranted = false   // the sender is too old, I don't vote for him
		return
	}

	voteSomeoneElse := !(rf.voteFor == -1 || rf.voteFor == args.CandidateId) // did I already vote for someone else?

	if (!voteSomeoneElse) && upToDate { // either I voted for nobody, or I voted for you already, and you log is more up-to-date than mine
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.ResetTimer()
		rf.voteFor = args.CandidateId // change my voteFor to the candidate
		return
	}

	// I already voted someone else, or you are not at least as up-to-date
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	return
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
	rf.logs = append(rf.logs, Log{rf.currentTerm, command}) // log to replicate to the cluster
	term := rf.currentTerm
	isLeader := rf.role == LEADER

	if isLeader {
		log.Printf("\n===============================New log from client to server %d with term %d===============================\n", rf.me, term)
		go rf.RequestReplication(index)
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
			return
		}
		//fmt.Printf("=================================[Server%d] MainRoutine Lock=================================\n", rf.me)
		rf.mu.Lock() // TODO: deadlock here
		role := rf.role
		//fmt.Printf("=================================[Server%d] MainRoutine Unlock=================================\n", rf.me)
		rf.mu.Unlock()
		//logMutex.Lock()
		//rf.LogServerStates()
		//logMutex.Unlock()
		switch role {
		case LEADER: // if you are a leader, then you should send heartbeats
			//fmt.Printf("\n=====================================================================================================\n")
			//fmt.Printf("=================================[Server%d] is LEADER=================================\n", rf.me)
			//fmt.Printf("=====================================================================================================\n")
			go rf.SendHeartbeats() // block here to ensure that no more than 10 heartbeat being sent in a second
			time.Sleep(100 * time.Millisecond)
			break
		case CANDIDATE: // if you are a candidate, you should start a election
			//fmt.Printf("\n=====================================================================================================\n")
			//fmt.Printf("=================================[Server%d] is CANDIDATE=================================\n", rf.me)
			//fmt.Printf("=====================================================================================================\n")
			rf.currentTerm++                  // increment my current term
			rf.voteFor = rf.me                // vote for myself
			go rf.RunElection()               // block here since we only proceed either with a election result or timeout
			time.Sleep(50 * time.Millisecond) // after sleep a while, and check if my timeout expires again
			break
		case FOLLOWER: // if you are a follower, you should do nothing but wait for RPC from your leader
			//fmt.Printf("\n=====================================================================================================\n")
			//fmt.Printf("=================================[Server%d] is FOLLOWER=================================\n", rf.me)
			//fmt.Printf("=====================================================================================================\n")
			if rf.electionExpired { // but first check election timeout
				rf.role = CANDIDATE // if it expires, then convert to candidate and proceed
			}
			time.Sleep(50 * time.Millisecond) // after sleep a while, and check if my timeout expires again
			break                             // if no timeout then it is fine, RPC call is handled in its handler so nothing to do here
		}
	}
}

//
// this function send AppendEntries RPCs to all its peers with no log
// it send RPC request for each server in a separate goroutine and handle RPC reply in a same goroutine;
// since labrpc guarantees that every RPC returns, we don't need to handle RPC timeout, etc.
// I don't think we need to wait for AppendEntries RPC to return, since we will handle the reply in SendAppendEntries
// and if the role changes, MainRoutine will know in the switch statement in the next iteration
//
func (rf *Raft) SendHeartbeats() {
	log.Printf("[server%d] enters SendHeartbeats\n", rf.me)
	defer log.Printf("[Server%d] quits SendHeartbeats", rf.me)

	rf.mu.Lock()                   // lock raft instance to prepare the RPC arguments
	term := rf.currentTerm         // my term
	leaderId := rf.me              // current leader, which is me
	leaderCommit := rf.commitIndex // last log I committed
	rf.mu.Unlock()                 // unlock raft when RPC arguments are prepared

	statusMutex := sync.Mutex{}
	status := make([]int, len(rf.peers)) // whether each peer's RPC succeed or not, all initialized to 0 (SENDING)

	for idx, _ := range rf.peers {
		if idx == leaderId {
			status[idx] = SUCCESS // I don't have to send heartbeat to myself, so my RPC is successful
			continue
		}
		rf.mu.Lock()                              // lock raft instance to prepare the prevLogIndex
		prevLogIndex := rf.nextIndex[idx] - 1     // index of log entry immediately preceding new ones (nextIndex-1)
		prevLogTerm := rf.logs[prevLogIndex].Term // term of prevLogIndex entry
		entries := make([]Log, 0)                 // TODO: heartbeat's entry should be determined by nextIndex?
		rf.mu.Unlock()                            // unlock raft when prevLogIndex are prepared
		args := AppendEntriesArgs{term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit}
		reply := AppendEntriesReply{-1, false}                                // if you see -1 in reply, then the receiver never receives the RPC
		go rf.SendAppendEntries(idx, args, reply, &status[idx], &statusMutex) // pass a copy instead of reference (I think args and reply may lost after it returns)
	}

	// no need to check RPC status since no need to retry for heartbeat, so it's fine if RPC failed
}

//
// this function send AppendEntries RPCs to all its peers with logs that need to be replicated
// it send RPC request for each server in a separate goroutine and handle RPC reply in a same goroutine;
// since labrpc guarantees that every RPC returns, we don't need to handle RPC timeout, etc.
// if majority of the cluster replied success, then the logs are regard as replicated
// then we commit these logs on the leader side, and followers will commit these in consequent AppendEntries RPCs
//
func (rf *Raft) RequestReplication(commandIndex int) {
	log.Printf("[Server%d] enters RequestReplication", rf.me)
	defer log.Printf("[Server%d] quits RequestReplication", rf.me)

	rf.mu.Lock()                   // lock raft instance to prepare the RPC arguments
	term := rf.currentTerm         // my term
	leaderId := rf.me              // current leader, which is me
	leaderCommit := rf.commitIndex // last log I committed
	rf.mu.Unlock()                 // unlock raft when RPC arguments are prepared

	statusMutex := sync.Mutex{}
	statuses := make([]int, len(rf.peers))                // whether each peer's RPC succeed or not, all initialized to 0 (SENDING)
	allArgs := make([]AppendEntriesArgs, len(rf.peers))   // slice to store all AppendEntries RPC argmuents
	allReply := make([]AppendEntriesReply, len(rf.peers)) // slice to store all AppendEntries RPC argmuents

	for idx, _ := range rf.peers {
		if idx == leaderId {
			statuses[idx] = SUCCESS // I don't have to send AppendEntries RPC to myself, so my RPC is successful
			continue                // I already appended this shit
		}
		rf.mu.Lock()                              // lock raft instance to prepare the prevLogIndex
		entries := rf.logs[rf.nextIndex[idx]:]    // nextIndex's possible maximal value is len(rf.logs), since we just append a new log, it won't exceed bound
		prevLogIndex := rf.nextIndex[idx] - 1     // index of log entry immediately preceding new ones (nextIndex-1)
		prevLogTerm := rf.logs[prevLogIndex].Term // term of prevLogIndex entry
		rf.mu.Unlock()                            // unlock raft when prevLogIndex are prepared
		args := AppendEntriesArgs{term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit}
		reply := AppendEntriesReply{-1, false} // if you see -1 in reply, then the receiver never receives the RPC
		allArgs[idx] = args
		allReply[idx] = reply
		go rf.SendAppendEntries(idx, args, reply, &statuses[idx], &statusMutex) // pass a copy instead of reference (I think args and reply may lost after it returns)
	}

	// TODO: use a chan with buffer to communicate so we know which RPC we should retry
	//go func(status []int, allArgs []AppendEntriesArgs, allReply []AppendEntriesReply) {
	//	for { // since ultimately there will be election timeout, so this goroutine eventually quits
	//		time.Sleep(30 * time.Millisecond) // sleep a while to save CPU and check role later
	//		statusMutex.Lock()
	//		for i, status := range status {
	//			if status == FAIL { // if RPC failed, then retry this RPC until it succeed
	//				go rf.SendAppendEntries(i, allArgs[i], allReply[i], &statuses[i], &statusMutex) // retry with exactly the same args
	//				_ = i
	//			}
	//		}
	//		statusMutex.Unlock()
	//	}
	//}(statuses, allArgs, allReply)
}

//
// send AppendEntries RPC to a single server and handle the reply
//
func (rf *Raft) SendAppendEntries(server int, args AppendEntriesArgs, reply AppendEntriesReply, result *int, statusMutex *sync.Mutex) {
	log.Printf("[Server%d] enters SendAppendEntries with prevLogIndex %d in term %d\n", rf.me, args.PrevLogIndex, args.Term)
	defer log.Printf("[Server%d] quits SendAppendEntries with prevLogIndex %d in term %d\n", rf.me, args.PrevLogIndex, args.Term)

	success := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	//fmt.Printf("=================================[Server%d] SendAppendEntries Status Lock=================================\n", rf.me)
	statusMutex.Lock()
	if !success { // RPC failed
		*result = FAIL // set status to FAIL
		log.Printf("AppendEntries from LEADER %d to FOLLOWER %d RPC failed\n", rf.me, server)
		//fmt.Printf("=================================[Server%d] SendAppendEntries Status Unlock=================================\n", rf.me)
		statusMutex.Unlock()
		// TODO: this might be where deadlock occurs
		return
	} else {
		*result = SUCCESS
	}
	//fmt.Printf("=================================[Server%d] SendAppendEntries Status Unlock=================================\n", rf.me)
	statusMutex.Unlock()

	//fmt.Printf("=================================[Server%d] SendAppendEntries Lock=================================\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer fmt.Printf("=================================[Server%d] SendAppendEntries Lock=================================\n", rf.me)
	defer rf.LogAppendEntriesSend(rf.me, server, &args, &reply)

	if reply.Term > rf.currentTerm { // I should no longer be the leader since my term is too old
		rf.currentTerm = reply.Term // set currentTerm = T
		rf.role = FOLLOWER          // convert to follower
		return
	}

	if reply.Success { // this follower's log now is matched to mine, so update the nextIndex and matchIndex
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries) // prevLog matches, and since success, entries just append also matches
		rf.nextIndex[server] = rf.matchIndex[server] + 1              // increment nextIndex by the number of entries just append
	} else { // this follower didn't catch up with my logs
		rf.nextIndex[server] = rf.nextIndex[server] - 1 // decrement nextIndex (since false means it doesn't match, matchIndex should still be 0)
	}

	if rf.matchIndex[server] != len(rf.logs)-1 { // if not matched, keep sending AppendEntries to force it to match
		// since this is re-sending, you need to reset you arguments since they might change
		term := rf.currentTerm // my term
		leaderId := rf.me      // current leader, which is me
		nextIndex := rf.nextIndex[server]
		entries := rf.logs[nextIndex:] // send the log entry with nextIndex, since follower doesn't match, nextIndex won't exceed len(rf.log)
		leaderCommit := rf.commitIndex
		prevLogIndex := rf.nextIndex[server] - 1
		prevLogTerm := rf.logs[prevLogIndex].Term
		args = AppendEntriesArgs{term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit}
		reply = AppendEntriesReply{-1, false}                             // if you see -1 in reply, then the receiver never receives the RPC
		go rf.SendAppendEntries(server, args, reply, result, statusMutex) // resend AppendEntries RPC
	}
}

//
// as a raft instance becomes a candidate, it asks all its peers to vote after it votes for itself
// it send RPC request for each server in a separate goroutine and handle RPC reply in a same goroutine;
// according to the result of the election, it decides whether it becomes the leader or convert to follower;
// since labrpc guarantees that every RPC returns, we don't need to handle RPC timeout, etc.
// This function quits only when I converts to follower or leader, or my election time expires so I should re-elect
//
func (rf *Raft) RunElection() {
	log.Printf("[Server%d] enters RunElection\n", rf.me)
	defer log.Printf("[Server%d] quits RunElection\n", rf.me)

	rf.mu.Lock()                              // lock raft instance to prepare the RPC arguments
	rf.ResetTimer()                           // reset the election timer because when starting an election
	term := rf.currentTerm                    // my term
	candidateId := rf.me                      // candidate id, which is me!
	lastLogIndex := len(rf.logs) - 1          // index of my last log entry
	lastLogTerm := rf.logs[lastLogIndex].Term // term of my last log entry
	rf.mu.Unlock()                            // unlock raft when RPC arguments are prepared

	//electionDone := make(chan int, 1)
	upVote := 1   // how many servers agree to vote for me (initialized to 1 since I vote for myself!)
	downVote := 0 // how many servers disagree to vote for me

	for idx, _ := range rf.peers {
		if idx == candidateId {
			continue // I don't have to vote for myself, I did this in my MainRoutine function
		}
		args := RequestVoteArgs{term, candidateId, lastLogIndex, lastLogTerm}
		reply := RequestVoteReply{-1, false}                        // if you see -1 in reply, then the receiver never receives the RPC
		go rf.SendRequestVote(idx, args, reply, &upVote, &downVote) // send RPC to each server
	}

	// TODO：this goroutine is fucking everyone up
	//go func(rf *Raft, done *chan int) { // once I am no longer a candidate or election time expires, I will immediately quit this function
	//	for { // since ultimately there will be election timeout, so this goroutine eventually quits
	//		rf.mu.Lock()                                    // lock raft instance to access its role
	//		if rf.role != CANDIDATE || rf.electionExpired { // check if I'm still a candidate and election timeout
	//			rf.mu.Unlock() // since you will return in this if section, release the lock now
	//			*done <- 1     // notify the function that the state is changed
	//			fmt.Printf("\n***************************[Server%d] election done***************************\n", rf.me)
	//			return // time to bail!
	//		}
	//		rf.mu.Unlock()
	//		time.Sleep(15 * time.Millisecond) // sleep a while to save CPU and check role later
	//	}
	//}(rf, &electionDone)

	//<-electionDone // wait for the election checker goroutine above
	//fmt.Printf("electionDone isn't blocked\n")

	//
	//rf.mu.Lock()
	//fmt.Printf("rf.mu.Lock() isn't blocked\n")
	//if rf.role == LEADER { // if I'm elected as leader ()
	//	fmt.Printf("[Server%d] has been elected as leader\n", rf.me)
	//	for idx, _ := range rf.nextIndex {
	//		rf.nextIndex[idx] = lastLogIndex + 1 // initialize nextIndex to leader last log index+1
	//		rf.matchIndex[idx] = 0               // initialize matchIndex to 0
	//	}
	//	go rf.SetCommitter() // set a committer to periodically check if the commitIndex can be incremented
	//}
	//rf.mu.Unlock()
}

//
// send RequestVote RPC to a single server and handle the reply
// also signal the voteDone cond var
//
func (rf *Raft) SendRequestVote(server int, args RequestVoteArgs, reply RequestVoteReply, upVote *int, downVote *int) {
	log.Printf("[Server%d] enters SendRequestVote in term %d\n", rf.me, args.Term)
	defer log.Printf("[Server%d] quits SendRequestVote in term %d\n", rf.me, args.Term)

	success := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	if !success { // RPC failed
		log.Printf("RequestVote from LEADER %d to FOLLOWER %d RPC failed\n", rf.me, server)
		return
	}
	// TODO: do we really have to discard reply with a different term that mismatches args' term?

	//fmt.Printf("=================================[Server%d] SendRequestVote Lock=================================\n", rf.me)
	rf.mu.Lock()         // add mutex lock before you access attributes of raft instance
	defer rf.mu.Unlock() // release mutex lock when the function quits
	//defer fmt.Printf("=================================[Server%d] SendRequestVote Unlock=================================\n", rf.me)
	defer rf.LogRequestVoteSend(rf.me, server, &args, &reply)

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

	if rf.role == CANDIDATE { // if there is already a result, then I don't need to check this shit
		if reply.VoteGranted { // this server agree to vote for me
			*upVote++
			if *upVote > len(rf.peers)/2 && reply.Term == rf.currentTerm { // I won majority in this term
				rf.role = LEADER // so I'm a leader now
				// initialized the leader
				for idx, _ := range rf.nextIndex {
					rf.nextIndex[idx] = args.LastLogIndex + 1 // initialize nextIndex to leader last log index+1
					rf.matchIndex[idx] = 0                    // initialize matchIndex to 0
				}
				go rf.SetCommitter()   // set a committer to periodically check if the commitIndex can be incremented
				go rf.SendHeartbeats() // upon election, send initial heartbeat to each server
			}
		} else { // this server agree to vote for me
			*downVote++
			if *downVote > len(rf.peers)/2 && reply.Term == rf.currentTerm { // I lost majority in this term
				rf.role = FOLLOWER // so I'm a follower now
			}
		}
	}
}

//
// the applier can be awaken by a cond var; every time there are logs are committed,
// you should awake the cond var and tell this function to apply logs
// which is to send log through applyCh, and also increase raft instance's lastApplied value
//
func (rf *Raft) SetApplier(applyCh chan ApplyMsg) {
	for {
		time.Sleep(100 * time.Millisecond)
		for {
			//fmt.Printf("=================================[Server%d] Applier Lock=================================\n", rf.me)
			rf.mu.Lock()
			if rf.lastApplied < rf.commitIndex { // there is log committed but not applied
				rf.lastApplied++ // now apply the next command
				//fmt.Printf("=================================[Server%d] Committer Unlock=================================\n", rf.me)
				rf.mu.Unlock()
				applyMsg := ApplyMsg{true, rf.logs[rf.lastApplied].Command, rf.lastApplied}
				applyCh <- applyMsg
			} else {
				//fmt.Printf("=================================[Server%d] Applier Unlock=================================\n", rf.me)
				rf.mu.Unlock()
				break
			}
		}
		if rf.killed() { // if the raft instance is killed, it means this test is finished and we should quit
			return
		}
	}
}

//
// only called when someone becomes leader
// the committer periodically check the following conditions to determine whether the leader's commitIndex should be incremented
// it first check whether you are the leader, because followers can only set commitIndex when receiving AppendEntries RPC
// then it check the following three conditions
// (1) there exists an N such that N > commitIndex; (2) a majority of matchIndex[i] >= N; (3) rf.logs[N].Term == rf.currentTerm
// if these conditions are satisfied, set commitIndex = N
// return when it's no longer leader
//
func (rf *Raft) SetCommitter() {
	for {
		//fmt.Printf("=================================[Server%d] Committer Lock=================================\n", rf.me)
		rf.mu.Lock()
		if rf.role != LEADER { // if I'm not leader, I have no right to increment commitIndex here
			//fmt.Printf("=================================[Server%d] Committer Unlock=================================\n", rf.me)
			rf.mu.Unlock()
			return
		}
		// N must not exceed log's bound
		for N := len(rf.logs) - 1; N > rf.commitIndex; N-- { // check if such N exists
			replicatedCnt := 1                            // how many servers have replicated this log, initialized to 1 since I already have this log
			for server, matchIdx := range rf.matchIndex { // count how many server replicated my log at N
				if server == rf.me { // no need to count me since I already replicated my log
					continue
				}
				if matchIdx >= N {
					replicatedCnt++ // one more server replicates log at N
				}
			}
			if replicatedCnt > len(rf.peers)/2 && rf.logs[N].Term == rf.currentTerm {
				rf.commitIndex = N
				break
			}
			N--
		}
		//fmt.Printf("=================================[Server%d] Committer Unlock=================================\n", rf.me)
		rf.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		if rf.killed() { // if the raft instance is killed, it means this test is finished and we should quit
			return
		}
	}
}

//
// the timer to calculate time passed since rf.electionLastTime
// if the time is more than a random threshold between 150ms to 300ms
// then there is a timeout, and the raft instance should set electionExpired to true
// to notify itself it should run a new election
//
func (rf *Raft) SetTimer() {
	rand.Seed(int64(rf.me))         // set a random number seed to ensure it generates different random number
	timeout := rand.Int()%300 + 150 // generate a random timeout threshold between 150 to 300ms
	for {
		if rf.killed() { // if the raft instance is killed, it means this test is finished and we should quit
			return
		}
		time.Sleep(10 * time.Millisecond) // sleep a while to save some CPU time
		electionCurrentTime := time.Now()
		rf.mu.Lock()
		if electionCurrentTime.Sub(rf.electionLastTime).Milliseconds() > int64(timeout) {
			rf.electionExpired = true // election time expired! you should run a new election now
			if rf.role != LEADER {
				log.Printf("Raft server %d's election time expired\n\n", rf.me)
			}
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

//
// why the fuck go don't have a integer min function???
//
func min(i, j int) int {
	if i < j {
		return i
	}
	return j
}
