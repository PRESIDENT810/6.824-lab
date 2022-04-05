package kvraft

const (
	GET    = 1
	PUT    = 2
	APPEND = 3
)

type Command struct {
	CommandType int
	Key         string
	Value       string
	CommandID   int64
}

// Get
//
// Handler for Get request from Clerk.
// Start a Get command to the raft instance held by this KVServer.
// If this raft instance is not the leader, then this function returns ErrWrongLeader immediately.
// If this raft instance is the leader, it appends a new entry to raft's log, and replicates this log via AppendEntries RPC,
// as this entry is committed and applied, ApplierReceiver will receive the applied command,
// and invoke the callback function we passed to CallbackController.
// If our callback function confirms that the OperationID is matched, then we can apply this operation via Storage layer.
// The storage layer will retrieve the corresponding value for the key, and such value is returned to Clerk
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	cmd := Command{GET, args.Key, "", args.ID}

	channel := make(chan bool)

	// Not sure where term should be used
	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// By comparing the commandID of the command we sent and the command actually applied,
	// we can determine whether our command is successfully broadcast to all raft peers
	callback := func(commandID int64) {
		if commandID != args.ID {
			channel <- false
		} else {
			channel <- true
		}
	}
	kv.controller.RegisterCallback(index, callback)

	success := <-channel
	if success {
		// The command applied is the one we sent in this function
		reply.Value = kv.storage.Get(args.Key)
		reply.Err = OK
		return
	} else {
		// The command applied is not the one we sent in this function
		// TODO: I don't think ErrWrongLeader explains what exactly happens here
		reply.Err = ErrWrongLeader
		return
	}

}

// Put
//
// Handler for Put request from Clerk.
// Start a Put command to the raft instance held by this KVServer.
// If this raft instance is not the leader, then this function returns ErrWrongLeader immediately.
// If this raft instance is the leader, it appends a new entry to raft's log, and replicates this log via AppendEntries RPC,
// as this entry is committed and applied, ApplierReceiver will receive the applied command,
// and invoke the callback function we passed to CallbackController.
// If our callback function confirms that the OperationID is matched, then we can apply this operation via Storage layer.
// The storage layer will set the value corresponding to this key, and then we can return to Clerk
func (kv *KVServer) Put(args *PutArgs, reply *PutReply) {
	// Your code here.
	cmd := Command{PUT, args.Key, args.Value, args.ID}

	channel := make(chan bool)

	// Not sure where term should be used
	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// By comparing the commandID of the command we sent and the command actually applied,
	// we can determine whether our command is successfully broadcast to all raft peers
	callback := func(commandID int64) {
		if commandID != args.ID {
			channel <- false
		} else {
			channel <- true
		}
	}
	kv.controller.RegisterCallback(index, callback)

	success := <-channel
	if success {
		// The command applied is the one we sent in this function
		kv.storage.Put(args.Key, args.Value)
		reply.Err = OK
		return
	} else {
		// The command applied is not the one we sent in this function
		// TODO: I don't think ErrWrongLeader explains what exactly happens here
		reply.Err = ErrWrongLeader
		return
	}

}

// Append
//
// Handler for Append request from Clerk.
// Start a Append command to the raft instance held by this KVServer.
// If this raft instance is not the leader, then this function returns ErrWrongLeader immediately.
// If this raft instance is the leader, it appends a new entry to raft's log, and replicates this log via AppendEntries RPC,
// as this entry is committed and applied, ApplierReceiver will receive the applied command,
// and invoke the callback function we passed to CallbackController.
// If our callback function confirms that the OperationID is matched, then we can apply this operation via Storage layer.
// The storage layer will append the value corresponding to this key, and then we can return to Clerk
func (kv *KVServer) Append(args *AppendArgs, reply *AppendReply) {
	// Your code here.
	cmd := Command{APPEND, args.Key, args.Value, args.ID}

	channel := make(chan bool)

	// Not sure where term should be used
	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// By comparing the commandID of the command we sent and the command actually applied,
	// we can determine whether our command is successfully broadcast to all raft peers
	callback := func(commandID int64) {
		if commandID != args.ID {
			channel <- false
		} else {
			channel <- true
		}
	}
	kv.controller.RegisterCallback(index, callback)

	success := <-channel
	if success {
		// The command applied is the one we sent in this function
		kv.storage.Append(args.Key, args.Value)
		reply.Err = OK
		return
	} else {
		// The command applied is not the one we sent in this function
		// TODO: I don't think ErrWrongLeader explains what exactly happens here
		reply.Err = ErrWrongLeader
		return
	}
}
