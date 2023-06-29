package kvraft

import (
	"6.5840/labrpc"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderServer int
	clientId     int64
	msgSeq       int64
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
	ck.leaderServer = 0
	ck.clientId = nrand()
	atomic.StoreInt64(&ck.msgSeq, 0)
	return ck
}

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
func (ck *Clerk) Get(key string) string {
	ck.msgSeq += 1
	args := &GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		MsgId:    ck.msgSeq,
	}
	reply := &GetReply{}

	leaderServer := ck.leaderServer
	for {
		ok := ck.servers[leaderServer].Call("KVServer.Get", args, reply)
		if ok {
			if reply.Err == OK {
				ck.leaderServer = leaderServer
				return reply.Value
			}
			if reply.Err == ErrNoKey {
				ck.leaderServer = leaderServer
				return ErrNoKey
			}
		}
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutDown {
			leaderServer = (leaderServer + 1) % len(ck.servers)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// You will have to modify this function.
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.msgSeq += 1
	args := &PutAppendArgs{
		Op:       op,
		Key:      key,
		Value:    value,
		ClientId: ck.clientId,
		MsgId:    ck.msgSeq,
	}
	reply := &PutAppendReply{}

	leaderServer := ck.leaderServer
	for {
		ok := ck.servers[leaderServer].Call("KVServer.PutAppend", args, reply)
		if ok && reply.Err == OK {
			ck.leaderServer = leaderServer
			return
		}
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutDown {
			leaderServer = (leaderServer + 1) % len(ck.servers)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
