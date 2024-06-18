package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	Database map[string]string
	History  map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	reply.Value = kv.Database[args.Key]
	kv.mu.Unlock()
	// Your code here.
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	// Your code here.
	_, ok := kv.History[args.HistoryId]
	if !ok {
		kv.Database[args.Key] = args.Value
		kv.History[args.HistoryId] = ""
	}
	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	// Your code here.
	if val, ok := kv.History[args.HistoryId]; !ok {
		reply.Value = kv.Database[args.Key]
		kv.Database[args.Key] += args.Value
		kv.History[args.HistoryId] = reply.Value
	} else {
		reply.Value = val
		log.Printf("%v\n", reply.Value)
	}
	kv.mu.Unlock()
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.Database = make(map[string]string)
	// You may need initialization code here.
	kv.History = make(map[string]string)
	return kv
}
