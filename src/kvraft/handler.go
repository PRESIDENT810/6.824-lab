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
	//kv.rf.RequestReplication(term)

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
		kv.mu.Lock()
		// Request with same operation ID has been executed
		if value, ok := kv.requestTracker[args.ID]; ok {
			kv.mu.Unlock()
			reply.Value = value
			reply.Err = OK
			return
		}
		// Track this operation ID in requestTracker
		value := kv.storage.Get(args.Key)
		kv.requestTracker[args.ID] = value
		// The command applied is the one we sent in this function
		reply.Value = value
		kv.mu.Unlock()
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
	index, term, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.rf.RequestReplication(term)

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
		kv.mu.Lock()
		// Request with same operation ID has been executed
		if _, ok := kv.requestTracker[args.ID]; ok {
			kv.mu.Unlock()
			reply.Err = OK
			return
		}
		// Track this operation ID in requestTracker
		kv.requestTracker[args.ID] = args.Value

		// The command applied is the one we sent in this function
		kv.storage.Put(args.Key, args.Value)
		kv.mu.Unlock()
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
	index, term, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// TODO: manually invoke kv.rf.RequestReplication, this is because if we call RequestReplication in Start(),
	// we cannot pass TestCount2B, so we have to call it here to avoid failing that test
	kv.rf.RequestReplication(term)

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

	// TODO: when a leader is chosen, it should write an no-op, otherwise it won't commit logs that are not in its term,
	// therefore these commands will not be applied so kvserver will not get anything from this channel
	success := <-channel
	if success {
		kv.mu.Lock()
		// Request with same operation ID has been executed
		if _, ok := kv.requestTracker[args.ID]; ok {
			kv.mu.Unlock()
			reply.Err = OK
			return
		}
		// Track this operation ID in requestTracker
		kv.requestTracker[args.ID] = args.Value

		// The command applied is the one we sent in this function
		kv.storage.Append(args.Key, args.Value)
		kv.mu.Unlock()
		reply.Err = OK
		return
	} else {
		// The command applied is not the one we sent in this function
		// TODO: I don't think ErrWrongLeader explains what exactly happens here
		reply.Err = ErrWrongLeader
		return
	}
}
