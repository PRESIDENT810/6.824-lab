package kvraft

const (
	ErrNoKey       = "ErrNoKey"
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type PutArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	ID int64
}

type PutReply struct {
	Err Err
}

type AppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	ID int64
}

type AppendReply struct {
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
