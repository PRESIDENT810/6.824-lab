package kvraft

const (
	ErrNoKey       = "ErrNoKey"
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// PutAppendArgs
//
// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	ID int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	ID int64
}

type GetReply struct {
	Err   Err
	Value string
}
