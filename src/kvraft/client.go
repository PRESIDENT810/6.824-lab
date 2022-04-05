package kvraft

import (
	"encoding/gob"
	"mit-6.824/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"
import "sync/atomic"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	// Keep track of who is the leader for the last RPC to speed up searching for leader
	lastKnownLeader int
	mu              sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastKnownLeader = 0
	gob.Register(Command{})
	return ck
}

var OperationID int64 = 0

// Get
//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	// Generate an unique operation identifier for each client request
	id := atomic.AddInt64(&OperationID, 1)

	serverIdx := ck.lastKnownLeader
	for {
		args := GetArgs{key, id}
		reply := GetReply{}
		ok := ck.servers[serverIdx].Call("KVServer.Get", &args, &reply)
		// If I sent an RPC to the wrong kvserver or cannot reach the kvserver, retry by sending to a different kvserver
		if !ok || reply.Err == ErrWrongLeader {
			serverIdx = (serverIdx + 1) % len(ck.servers)
			continue
		}
		if reply.Err == ErrNoKey {
			return ""
		}
		if reply.Err == OK {
			ck.mu.Lock()
			ck.lastKnownLeader = serverIdx
			ck.mu.Unlock()
			return reply.Value
		}
		panic("Unrecognized reply error")
	}

	return ""
}

// Put
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Put", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Put(key string, value string) {
	// You will have to modify this function.

	// Generate an unique operation identifier for each client request
	id := atomic.AddInt64(&OperationID, 1)

	serverIdx := ck.lastKnownLeader
	for {
		args := PutArgs{key, value, id}
		reply := PutReply{}
		ok := ck.servers[serverIdx].Call("KVServer.Put", &args, &reply)
		// If I sent an RPC to the wrong kvserver or cannot reach the kvserver, retry by sending to a different kvserver
		if !ok || reply.Err == ErrWrongLeader {
			serverIdx = (serverIdx + 1) % len(ck.servers)
			continue
		}
		if reply.Err == OK {
			ck.mu.Lock()
			ck.lastKnownLeader = serverIdx
			ck.mu.Unlock()
			return
		}
		panic("Unrecognized reply error")
	}

	return
}

// Append
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Append", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Append(key string, value string) {
	// You will have to modify this function.

	// Generate an unique operation identifier for each client request
	id := atomic.AddInt64(&OperationID, 1)

	serverIdx := ck.lastKnownLeader
	for {
		args := AppendArgs{key, value, id}
		reply := AppendReply{}
		ok := ck.servers[serverIdx].Call("KVServer.Append", &args, &reply)
		// If I sent an RPC to the wrong kvserver or cannot reach the kvserver, retry by sending to a different kvserver
		if !ok || reply.Err == ErrWrongLeader {
			serverIdx = (serverIdx + 1) % len(ck.servers)
			continue
		}
		if reply.Err == OK {
			ck.mu.Lock()
			ck.lastKnownLeader = serverIdx
			ck.mu.Unlock()
			return
		}
		panic("Unrecognized reply error")
	}

	return
}
