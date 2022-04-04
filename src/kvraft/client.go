package kvraft

import "mit-6.824/labrpc"
import "crypto/rand"
import "math/big"
import "sync/atomic"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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

	args := GetArgs{key, id}
	reply := GetReply{}

	serverIdx := -1
	for {
		serverIdx = (serverIdx + 1) % len(ck.servers)
		ok := ck.servers[serverIdx].Call("KVServer.Get", &args, &reply)
		// If I sent an RPC to the wrong kvserver or cannot reach the kvserver, retry by sending to a different kvserver
		if !ok || reply.Err == ErrWrongLeader {
			continue
		}
		if reply.Err == ErrNoKey {
			return ""
		}
		if reply.Err == OK {
			return reply.Value
		}
		panic("Unrecognized reply error")
	}

	return ""
}

// PutAppend
//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	// Generate an unique operation identifier for each client request
	id := atomic.AddInt64(&OperationID, 1)

	args := PutAppendArgs{key, value, op, id}
	reply := PutAppendReply{}

	serverIdx := -1
	for {
		serverIdx = (serverIdx + 1) % len(ck.servers)
		ok := ck.servers[serverIdx].Call("KVServer.PutAppend", &args, &reply)
		// If I sent an RPC to the wrong kvserver or cannot reach the kvserver, retry by sending to a different kvserver
		if !ok || reply.Err == ErrWrongLeader {
			continue
		}
		if reply.Err == OK {
			return
		}
		panic("Unrecognized reply error")
	}

	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
