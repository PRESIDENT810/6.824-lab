package kvraft

import (
	"mit-6.824/raft"
	"sync"
)

// CallbackController
//
// When a command with CommandIndex i is applied, we should invoke all callback functions in CallbackMap[i]
// Each callback function captures three variable from upper level callers (Get, Put or Append):
// index (int), OperationID (int) and channel (chan bool)
// When a callback function is invoked, we first check whether OperationID matches the one obtain from applied command
// If it doesn't match, we pass false to chan bool, so the caller in handler.go will know that this operation
// (one of Get, Put or Append) fails, so it should return false; otherwise, if OperationID matches, it means
// all raft server acknowledges this operation, which indicates that linearization is ensures, so upper level caller
// (one of Get, Put or Append) can retrieve result from kvserver's Storage and return successfully
type CallbackController struct {
	CallbackMap map[int][]func(int64)
	mu          sync.Mutex
	me          int
}

// RegisterCallback
//
// Whenever kvserver calls raft's Start function, it should first register a callback via this function.
// This function register a callback function to be invoked when raft applies a command with the same index
func (controller *CallbackController) RegisterCallback(index int, callback func(commandID int64)) {
	controller.mu.Lock()
	controller.LogCallbackMutexLock()
	defer controller.LogCallbackMutexUnlock()
	defer controller.mu.Unlock()
	controller.LogRegisterCallback(index)
	if _, ok := controller.CallbackMap[index]; ok {
		controller.CallbackMap[index] = append(controller.CallbackMap[index], callback)
		return
	}
	controller.CallbackMap[index] = []func(int64){callback}
	return
}

// ApplierReceiver
//
// This function periodically check the command applied by raft corresponding to the kvserver holding this controller.
// If a command get applied, ApplierReceiver receives this command via applyCh, then it finds all callbacks registered
// for the same index of this command, invoking each of them and passing the CommandID in the command
func (kv *KVServer) ApplierReceiver(applyCh chan raft.ApplyMsg) {
	// Handle received applied message
	for applyMsg := range applyCh {
		kv.controller.mu.Lock()
		kv.controller.LogCallbackMutexLock()

		commandValid := applyMsg.CommandValid
		command := applyMsg.Command.(Command)
		commandIndex := applyMsg.CommandIndex

		if commandValid { // Normal command
			// An applied command could have multiple callback for that index, because a submitted log entry
			// could be discarded (see figure 8); however, there can't be an applied command with no callback,
			// because a command must be submitted with a callback registered in previous
			for _, callback := range kv.controller.CallbackMap[commandIndex] {
				kv.controller.LogConsumeCallback(commandIndex)
				go callback(command.CommandID)
			}
			delete(kv.controller.CallbackMap, commandIndex)
		} else { // Snapshot
			// TODO: implement this in lab 3B
		}

		kv.controller.LogCallbackMutexUnlock()
		kv.controller.mu.Unlock()
	}
}
