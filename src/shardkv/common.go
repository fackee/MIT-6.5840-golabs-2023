package shardkv

import "log"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"

	ErrWrongConfigNum = "ErrWrongConfigNum"
	ErrMigrating      = "ErrMigrating"

	ErrTimeout  = "ErrTimeout"
	ErrShutDown = "ErrShutDown"
)

type Err string

type ShardMigrateArgs struct {
	ConfigNum int
	ShardNum  int
}

type ShardMigrateReply struct {
	Err         Err
	ConfigNum   int
	Data        map[string]string
	LastApplied map[int64]int64
}

func (sr *ShardMigrateReply) Copy() ShardMigrateReply {
	data := make(map[string]string)
	for k, v := range sr.Data {
		data[k] = v
	}
	cmds := make(map[int64]int64)
	for k, v := range sr.LastApplied {
		cmds[k] = v
	}
	return ShardMigrateReply{
		ConfigNum:   sr.ConfigNum,
		Data:        data,
		LastApplied: cmds,
	}
}

type GarbageCollectArgs struct {
	ConfigNum int
	ShardNum  int
}

type GarbageCollectReply struct {
	Err Err
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	MessageId int64
	ClientId  int64
	ConfigNum int
	ShardNum  int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	MessageId int64
	ConfigNum int
	ShardNum  int
}

type GetReply struct {
	Err   Err
	Value string
}

func Max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

const Debug = false

func DPrint(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}
