package kvraft

import (
	"log"
	"reflect"
	"sync"
)

const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
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
	true,
	true,
	false,
	true,
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
	LogMutex.Lock()
	defer LogMutex.Unlock()
	if logConfig.EnableClerk {
		DPrintf("\n====================================================================\n")
		DPrintf("[Clerk] enters %v\n", RequestType)
		DPrintf("====================================================================\n")
	}
}

func (kv *KVServer) LogKVServerIn(RequestType string, args interface{}, reply interface{}) {
	LogMutex.Lock()
	defer LogMutex.Unlock()
	if logConfig.EnableKVServer {
		DPrintf("\n====================================================================\n")
		DPrintf("[KVServer %d] enters %v\n", kv.me, RequestType)
		LogStruct(args)
		LogStruct(reply)
		DPrintf("====================================================================\n")
	}
}

func (kv *KVServer) LogKVServerOut(RequestType string, args interface{}, reply interface{}) {
	LogMutex.Lock()
	defer LogMutex.Unlock()
	if logConfig.EnableKVServer {
		DPrintf("\n====================================================================\n")
		DPrintf("[KVServer %d] exits %v\n", kv.me, RequestType)
		LogStruct(args)
		LogStruct(reply)
		DPrintf("====================================================================\n")
	}
}

func (controller *CallbackController) LogCallbackMutexLock() {
	LogMutex.Lock()
	defer LogMutex.Unlock()
	if logConfig.EnableMutex {
		DPrintf("[CallbackController %d] locked\n", controller.me)
	}
}

func (controller *CallbackController) LogCallbackMutexUnlock() {
	LogMutex.Lock()
	defer LogMutex.Unlock()
	if logConfig.EnableMutex {
		DPrintf("[CallbackController %d] unlocked\n", controller.me)
	}
}

func (ms *MapStorage) LogStorageMutexLock() {
	LogMutex.Lock()
	defer LogMutex.Unlock()
	if logConfig.EnableMutex {
		DPrintf("[MapStorage %d] locked\n", ms.me)
	}
}

func (ms *MapStorage) LogStorageMutexUnlock() {
	LogMutex.Lock()
	defer LogMutex.Unlock()
	if logConfig.EnableMutex {
		DPrintf("[MapStorage %d] unlocked\n", ms.me)
	}
}

func (ms *MapStorage) LogStorage(RequestType string) {
	LogMutex.Lock()
	defer LogMutex.Unlock()
	if logConfig.EnableStorage {
		DPrintf("[Storage] enters %v\n", RequestType)
		DPrintf("%v\n", ms.m)
	}
}
