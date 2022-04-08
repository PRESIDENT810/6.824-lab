package kvraft

import (
	"fmt"
	"os"
	"reflect"
	"sync"
)

const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		fmt.Printf(format, a...)
	}
	return
}

func init() {
	DebugKVRaft := os.Getenv("DEBUG_KV")
	if DebugKVRaft != "ON" {
		logConfig = LogConfig{
			false,
			false,
			false,
			false,
			false,
		}
	}
}

var LogMutex sync.Mutex

type LogConfig struct {
	EnableClerk    bool
	EnableKVServer bool
	EnableStorage  bool
	EnableCallback bool
	EnableMutex    bool
}

var logConfig = LogConfig{
	false,
	false,
	false,
	false,
	false,
}

func LogStruct(args interface{}) {
	t := reflect.TypeOf(args)
	name := t.Name()
	n := t.NumField()
	v := reflect.ValueOf(args)
	DPrintf("------------------------%v------------------------\n", name)
	for i := 0; i < n; i++ {
		fieldName := t.Field(i).Name
		fieldValue := v.FieldByName(fieldName)
		DPrintf("%v=%v\n", fieldName, fieldValue)
	}
	DPrintf("\n")
}

func (ck *Clerk) LogClerk(RequestType string, key, string, value string) {
	if logConfig.EnableClerk {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		DPrintf("\n====================================================================\n")
		DPrintf("[Clerk] enters %v\n", RequestType)
		DPrintf("====================================================================\n")
	}
}

func (kv *KVServer) LogKVServerIn(RequestType string, args interface{}, reply interface{}) {
	if logConfig.EnableKVServer {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		DPrintf("\n====================================================================\n")
		DPrintf("[KVServer %d] enters %v\n", kv.me, RequestType)
		LogStruct(args)
		LogStruct(reply)
		DPrintf("====================================================================\n")
	}
}

func (kv *KVServer) LogKVServerOut(RequestType string, args interface{}, reply interface{}) {
	if logConfig.EnableKVServer {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		DPrintf("\n====================================================================\n")
		DPrintf("[KVServer %d] exits %v\n", kv.me, RequestType)
		LogStruct(args)
		LogStruct(reply)
		DPrintf("====================================================================\n")
	}
}

func (controller *CallbackController) LogRegisterCallback(index int) {
	if logConfig.EnableCallback {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		DPrintf("\n====================================================================\n")
		DPrintf("[CallbackController %d] registers callback for index%d\n", controller.me, index)
		DPrintf("====================================================================\n")
	}
}

func (controller *CallbackController) LogConsumeCallback(index int) {
	if logConfig.EnableCallback {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		DPrintf("\n====================================================================\n")
		DPrintf("[CallbackController %d] consumes callback for index%d\n", controller.me, index)
		DPrintf("====================================================================\n")
	}
}

func (controller *CallbackController) LogCallbackMutexLock() {
	if logConfig.EnableMutex {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		DPrintf("[CallbackController %d] locked\n", controller.me)
	}
}

func (controller *CallbackController) LogCallbackMutexUnlock() {
	if logConfig.EnableMutex {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		DPrintf("[CallbackController %d] unlocked\n", controller.me)
	}
}

func (ms *MapStorage) LogStorageMutexLock() {
	if logConfig.EnableMutex {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		DPrintf("[MapStorage %d] locked\n", ms.me)
	}
}

func (ms *MapStorage) LogStorageMutexUnlock() {
	if logConfig.EnableMutex {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		DPrintf("[MapStorage %d] unlocked\n", ms.me)
	}
}

func (ms *MapStorage) LogStorage(RequestType string) {
	if logConfig.EnableStorage {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		DPrintf("[Storage] enters %v\n", RequestType)
		DPrintf("%v\n", ms.m)
	}
}
