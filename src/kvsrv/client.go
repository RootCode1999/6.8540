package kvsrv

import (
	"crypto/rand"
	"encoding/base64"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key: key,
	}
	reply := GetReply{}
	ok := ck.server.Call("KVServer.Get", &args, &reply)
	for {
		if ok == true {
			break
		}
		time.Sleep(100 * time.Millisecond)
		ok = ck.server.Call("KVServer.Get", &args, &reply)
	}
	// You will have to modify this function.
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	historyId := generatString()
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		HistoryId: historyId,
	}
	reply := PutAppendReply{}
	ok := ck.server.Call("KVServer."+op, &args, &reply)
	for {
		if ok == true {
			break
		}
		time.Sleep(100 * time.Millisecond)
		ok = ck.server.Call("KVServer."+op, &args, &reply)
	}
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}

func generatString() string {
	// 生成一个64字节的随机字符串
	randomBytes := make([]byte, 6)
	// 生成安全的随机字节
	_, err := rand.Read(randomBytes)
	for err != nil {
		_, err = rand.Read(randomBytes)
	}
	// 将字节切片编码为base64字符串
	// URLEncoding 用于生成一个 URL 安全的字符串
	randomString := base64.StdEncoding.EncodeToString(randomBytes)
	return randomString
}
