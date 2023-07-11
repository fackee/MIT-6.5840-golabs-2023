package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"bytes"
	"sync/atomic"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

const PullConfigDuration = time.Millisecond * 200
const MigrateDuration = time.Millisecond * 100
const GarbageCollectDuration = time.Millisecond * 150

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	ConfigNum int
	ShardNum  int
	Key       string
	Value     string
	MessageId int64
	RequestId int64
	ClientId  int64
	Term      int

	//Apply Config
	ActiveConfig shardctrler.Config
	NewlyConfig  shardctrler.Config

	//Sync
	Data        map[string]string
	LastApplied map[int64]int64
}

type ApplyMsg struct {
	Value     string
	Error     Err
	ApplyTerm int
}

type GarbageTemper struct {
	Data        map[string]string
	LastApplied map[int64]int64
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead      int32 // set by Kill()
	persister *raft.Persister
	mck       *shardctrler.Clerk
	opChan    map[int64]chan ApplyMsg
	killCh    chan struct{}

	// persistent data
	Db              [shardctrler.NShards]map[string]string
	LastApplied     map[int64]int64
	ServingShards   map[int]struct{}
	WaitingShards   map[int][]string
	GarbageSnapshot map[int]map[int]GarbageTemper
	GarbageList     map[int]map[int][]string
	ActiveConfigNum int
}

func (kv *ShardKV) isRepeated(clientId int64, msgId int64) bool {
	if maxSeqId, ok := kv.LastApplied[clientId]; ok {
		return msgId <= maxSeqId
	}
	return false
}

func (kv *ShardKV) isNormalOp(op string) bool {
	return op == "Get" || op == "Put" || op == "Append"
}

func (kv *ShardKV) isConfig(op string) bool {
	return op == "Config"
}

func (kv *ShardKV) isSync(op string) bool {
	return op == "Sync"
}

func (kv *ShardKV) isGarbageCollect(op string) bool {
	return op == "GarbageCollect"
}

func (kv *ShardKV) isGarbageListCollect(op string) bool {
	return op == "GarbageListCollect"
}

func (kv *ShardKV) getData(shardNum int, key string) (err Err, value string) {
	if v, ok := kv.Db[shardNum][key]; ok {
		err = OK
		value = v
		return
	} else {
		err = ErrNoKey
		return
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Operation: "Get",
		Key:       args.Key,
		ConfigNum: args.ConfigNum,
		ShardNum:  args.ShardNum,
		ClientId:  args.ClientId,
		RequestId: nrand(),
	}
	res := kv.executeCommand(op)
	reply.Err = res.Error
	reply.Value = res.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Operation: args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ConfigNum: args.ConfigNum,
		ShardNum:  args.ShardNum,
		ClientId:  args.ClientId,
		MessageId: args.MessageId,
		RequestId: nrand(),
	}
	res := kv.executeCommand(op)
	reply.Err = res.Error
}

func (kv *ShardKV) ShardMigrate(args *ShardMigrateArgs, reply *ShardMigrateReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
	kv.mu.Lock()
	if args.ConfigNum > kv.ActiveConfigNum {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if shard2data, ok := kv.GarbageSnapshot[args.ConfigNum]; ok {
		if temper, ok := shard2data[args.ShardNum]; ok {
			reply.Data = temper.Data
			reply.LastApplied = temper.LastApplied
		}
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) ShardCollect(args *GarbageCollectArgs, reply *GarbageCollectReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//kv.mu.Lock()
	//if args.ConfigNum < kv.ActiveConfig.Num {
	//	reply.Err = ErrWrongConfigNum
	//	kv.mu.Unlock()
	//	return
	//}
	//kv.mu.Unlock()
	reply.Err = OK
	kv.rf.Start(Op{
		Operation: "GarbageCollect",
		ConfigNum: args.ConfigNum,
		ShardNum:  args.ShardNum,
	})
}

func (kv *ShardKV) executeCommand(op Op) (res ApplyMsg) {
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		res.Error = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := make(chan ApplyMsg, 1)
	kv.opChan[op.RequestId] = ch
	kv.mu.Unlock()
	select {
	case <-time.After(500 * time.Millisecond):
		res.Error = ErrTimeout
		return
	case res = <-ch:
		return
	case <-kv.killCh:
		res.Error = ErrShutDown
		return
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	kv.killCh <- struct{}{}
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) decodeSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	w := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(w)
	if d.Decode(&kv.Db) != nil ||
		d.Decode(&kv.LastApplied) != nil ||
		d.Decode(&kv.ServingShards) != nil ||
		d.Decode(&kv.WaitingShards) != nil ||
		d.Decode(&kv.GarbageSnapshot) != nil ||
		d.Decode(&kv.GarbageList) != nil ||
		d.Decode(&kv.ActiveConfigNum) != nil {
		panic("decode err")
	}
}

func (kv *ShardKV) doSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	if e.Encode(kv.Db) != nil ||
		e.Encode(kv.LastApplied) != nil ||
		e.Encode(kv.ServingShards) != nil ||
		e.Encode(kv.WaitingShards) != nil ||
		e.Encode(kv.GarbageSnapshot) != nil ||
		e.Encode(kv.GarbageList) != nil ||
		e.Encode(kv.ActiveConfigNum) != nil {
		panic("encode err")
	}
	kv.mu.Unlock()
	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *ShardKV) SendMigrateRequest(servers []string, shardNum int, configNum int) {
	args := ShardMigrateArgs{
		ConfigNum: configNum,
		ShardNum:  shardNum,
	}
	for si := 0; si < len(servers); si++ {
		srv := kv.make_end(servers[si])
		var reply ShardMigrateReply
		ok := srv.Call("ShardKV.ShardMigrate", &args, &reply)
		if ok && reply.Err == OK {
			kv.mu.Lock()
			if _, ok := kv.WaitingShards[shardNum]; !ok || kv.ActiveConfigNum != configNum {
				kv.mu.Unlock()
				return
			}
			replyCopy := reply.Copy()
			kv.rf.Start(Op{
				Operation:   "Sync",
				ConfigNum:   configNum,
				ShardNum:    shardNum,
				Data:        replyCopy.Data,
				LastApplied: replyCopy.LastApplied,
			})
			kv.mu.Unlock()
			return
		}
	}
}

func (kv *ShardKV) SendGarbageCollectRequest(servers []string, shardNum int, configNum int) {
	args := GarbageCollectArgs{
		ConfigNum: configNum,
		ShardNum:  shardNum,
	}
	for si := 0; si < len(servers); si++ {
		srv := kv.make_end(servers[si])
		var reply GarbageCollectReply
		ok := srv.Call("ShardKV.ShardCollect", &args, &reply)
		if ok && reply.Err == OK {
			kv.rf.Start(Op{
				Operation: "GarbageListCollect",
				ConfigNum: configNum,
				ShardNum:  shardNum,
			})
			return
		}
	}
}

func (kv *ShardKV) pushGarbageCollect() {
	for !kv.killed() {
		select {
		case <-kv.killCh:
			return
		default:
			_, isLeader := kv.rf.GetState()
			kv.mu.Lock()
			if isLeader && len(kv.GarbageList) > 0 {
				var wait sync.WaitGroup
				for configNum, shardNum2Remotes := range kv.GarbageList {
					for shardNum, remotes := range shardNum2Remotes {
						wait.Add(1)
						go func(cfgNum int, sdNum int, servers []string) {
							defer wait.Done()
							kv.SendGarbageCollectRequest(servers, sdNum, cfgNum)
						}(configNum, shardNum, remotes)
					}
				}
				kv.mu.Unlock()
				wait.Wait()
			} else {
				kv.mu.Unlock()
			}
		}
		time.Sleep(GarbageCollectDuration)
	}
}

func (kv *ShardKV) migrate() {
	for !kv.killed() {
		select {
		case <-kv.killCh:
			return
		default:
			_, isLeader := kv.rf.GetState()
			kv.mu.Lock()
			if isLeader && len(kv.WaitingShards) > 0 {
				configNum := kv.ActiveConfigNum
				var wait sync.WaitGroup
				DPrint("[%v-%v]Pulling New Shards:%v\n", kv.gid, kv.me, kv.WaitingShards)
				for shard, servers := range kv.WaitingShards {
					wait.Add(1)
					go func(shardNum int, remotes []string) {
						defer wait.Done()
						kv.SendMigrateRequest(remotes, shardNum, configNum)
					}(shard, servers)
				}
				kv.mu.Unlock()
				wait.Wait()
			} else {
				kv.mu.Unlock()
			}
		}
		time.Sleep(MigrateDuration)
	}
}

func (kv *ShardKV) pullConfig() {
	for !kv.killed() {
		select {
		case <-kv.killCh:
			return
		default:
			_, isLeader := kv.rf.GetState()
			kv.mu.Lock()
			if isLeader && len(kv.WaitingShards) == 0 {
				activeConfigNum := kv.ActiveConfigNum
				kv.mu.Unlock()
				oldConfig := kv.mck.Query(activeConfigNum)
				newlyConfig := kv.mck.Query(activeConfigNum + 1)
				if newlyConfig.Num == oldConfig.Num+1 {
					DPrint("[%v-%v]Pulling New Config:%v\n", kv.gid, kv.me, newlyConfig)
					kv.rf.Start(Op{
						Operation:    "Config",
						ActiveConfig: oldConfig.Copy(),
						NewlyConfig:  newlyConfig.Copy(),
					})
				}
			} else {
				kv.mu.Unlock()
			}
		}
		time.Sleep(PullConfigDuration)
	}
}

func (kv *ShardKV) apply() {
	for !kv.killed() {
		select {
		case <-kv.killCh:
			return
		case msg := <-kv.applyCh:
			if msg.SnapshotValid {
				kv.mu.Lock()
				kv.decodeSnapshot(msg.Snapshot)
				kv.mu.Unlock()
				continue
			}
			op := msg.Command.(Op)
			kv.mu.Lock()
			applyMsg := ApplyMsg{}
			if kv.isNormalOp(op.Operation) {
				err, v := kv.applyNormalOp(op)
				applyMsg.Error = err
				applyMsg.Value = v
			} else if kv.isConfig(op.Operation) {
				err := kv.applyConfig(op)
				applyMsg.Error = err
			} else if kv.isSync(op.Operation) {
				err := kv.applySync(op)
				applyMsg.Error = err
			} else if kv.isGarbageCollect(op.Operation) {
				err := kv.applyGarbageCollect(op)
				applyMsg.Error = err
			} else if kv.isGarbageListCollect(op.Operation) {
				err := kv.applyGarbageListCollect(op)
				applyMsg.Error = err
			} else {
				panic("error operation")
			}
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
				go kv.doSnapshot(msg.CommandIndex)
			}
			if ch, ok := kv.opChan[op.RequestId]; ok {
				ch <- applyMsg
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) applyNormalOp(op Op) (Err, string) {
	if kv.ActiveConfigNum == 0 || op.ConfigNum != kv.ActiveConfigNum {
		return ErrWrongGroup, ""
	}
	if _, ok := kv.ServingShards[op.ShardNum]; !ok {
		return ErrWrongGroup, ""
	}
	if _, ok := kv.WaitingShards[op.ShardNum]; ok {
		return ErrWrongGroup, ""
	}
	_, value := kv.getData(op.ShardNum, op.Key)
	if "Get" == op.Operation {
		return OK, value
	}
	isRepeated := kv.isRepeated(op.ClientId, op.MessageId)
	if !isRepeated {
		switch op.Operation {
		case "Put":
			kv.Db[op.ShardNum][op.Key] = op.Value
		case "Append":
			kv.Db[op.ShardNum][op.Key] = value + op.Value
		}
		kv.LastApplied[op.ClientId] = op.MessageId
	}
	return OK, ""
}

func (kv *ShardKV) applyConfig(op Op) Err {
	if kv.ActiveConfigNum+1 != op.NewlyConfig.Num {
		return ErrWrongConfigNum
	}
	if len(kv.WaitingShards) > 0 {
		return ErrMigrating
	}
	newConfig := op.NewlyConfig
	oldConfig := op.ActiveConfig

	kv.WaitingShards = make(map[int][]string)
	kv.ServingShards = make(map[int]struct{})

	snapshotCommands := make(map[int64]int64)
	for k, v := range kv.LastApplied {
		snapshotCommands[k] = v
	}
	for i := 0; i < shardctrler.NShards; i++ {
		if newConfig.Shards[i] == kv.gid {
			kv.ServingShards[i] = struct{}{}
			if kv.ActiveConfigNum != 0 && oldConfig.Shards[i] != kv.gid {
				kv.WaitingShards[i] = oldConfig.Groups[oldConfig.Shards[i]]
			}
		} else {
			if oldConfig.Shards[i] == kv.gid {
				snapshotData := make(map[string]string)
				for k, v := range kv.Db[i] {
					snapshotData[k] = v
				}
				kv.Db[i] = make(map[string]string)
				if _, ok := kv.GarbageSnapshot[newConfig.Num]; !ok {
					kv.GarbageSnapshot[newConfig.Num] = make(map[int]GarbageTemper)
				}
				kv.GarbageSnapshot[newConfig.Num][i] = GarbageTemper{
					Data:        snapshotData,
					LastApplied: snapshotCommands,
				}
			}
		}
	}
	kv.ActiveConfigNum = newConfig.Num
	return OK
}

func (kv *ShardKV) applySync(op Op) Err {
	if op.ConfigNum != kv.ActiveConfigNum {
		return ErrWrongConfigNum
	}
	if _, ok := kv.WaitingShards[op.ShardNum]; !ok {
		return OK
	}
	for k, v := range op.Data {
		kv.Db[op.ShardNum][k] = v
	}
	for k, v := range op.LastApplied {
		if oldValue, ok := kv.LastApplied[k]; !ok || v > oldValue {
			kv.LastApplied[k] = v
		}
	}
	if _, ok := kv.GarbageList[op.ConfigNum]; !ok {
		kv.GarbageList[op.ConfigNum] = make(map[int][]string)
	}
	kv.GarbageList[op.ConfigNum][op.ShardNum] = kv.WaitingShards[op.ShardNum]
	delete(kv.WaitingShards, op.ShardNum)
	return OK
}

func (kv *ShardKV) applyGarbageCollect(op Op) Err {
	if _, ok := kv.GarbageSnapshot[op.ConfigNum]; ok {
		delete(kv.GarbageSnapshot[op.ConfigNum], op.ShardNum)
		if len(kv.GarbageSnapshot[op.ConfigNum]) == 0 {
			delete(kv.GarbageSnapshot, op.ConfigNum)
		}
	}
	return OK
}

func (kv *ShardKV) applyGarbageListCollect(op Op) Err {
	if _, ok := kv.GarbageList[op.ConfigNum]; ok {
		delete(kv.GarbageList[op.ConfigNum], op.ShardNum)
		if len(kv.GarbageList[op.ConfigNum]) == 0 {
			delete(kv.GarbageList, op.ConfigNum)
		}
	}
	return OK
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister

	kv.opChan = make(map[int64]chan ApplyMsg)
	kv.LastApplied = make(map[int64]int64)
	kv.killCh = make(chan struct{})

	kv.Db = [shardctrler.NShards]map[string]string{}
	for i, _ := range kv.Db {
		kv.Db[i] = make(map[string]string)
	}
	kv.LastApplied = make(map[int64]int64)
	kv.ServingShards = make(map[int]struct{})
	kv.WaitingShards = make(map[int][]string)
	kv.GarbageSnapshot = make(map[int]map[int]GarbageTemper)
	kv.GarbageList = make(map[int]map[int][]string)
	kv.ActiveConfigNum = 0

	snapshot := kv.persister.ReadSnapshot()
	if snapshot != nil && len(snapshot) > 0 {
		kv.decodeSnapshot(snapshot)
	}

	// You may need initialization code here.
	go kv.apply()

	go kv.pullConfig()

	go kv.migrate()

	go kv.pushGarbageCollect()

	return kv
}
