package shardctrler

import (
	"6.5840/raft"
	"sort"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	opChan      map[int64]chan Config
	LastCommand map[int64]int64
	killedChan  chan struct{}

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	RequestId int64
	MessageId int64
	Operation string
	Servers   map[int][]string
	Shard     int
	GID       int
	GIDs      []int
	ConfigNum int
}

func (cfg *Config) deepCopy() (map[int][]string, [NShards]int) {
	groups := make(map[int][]string)
	for k, v := range cfg.Groups {
		groups[k] = v
	}
	shards := [NShards]int{}
	for i, gid := range cfg.Shards {
		shards[i] = gid
	}
	return groups, shards
}

func (sc *ShardCtrler) lastedConfig() Config {
	return sc.configs[len(sc.configs)-1]
}
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		RequestId: nrand(),
		MessageId: args.MessageId,
		Operation: "Join",
		Servers:   args.Servers,
	}
	err, _ := sc.executeCommand(op)
	reply.Err = err
	reply.WrongLeader = err == ErrWrongLeader
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		RequestId: nrand(),
		MessageId: args.MessageId,
		Operation: "Leave",
		GIDs:      args.GIDs,
	}
	err, _ := sc.executeCommand(op)
	reply.Err = err
	reply.WrongLeader = err == ErrWrongLeader
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		RequestId: nrand(),
		MessageId: args.MessageId,
		Operation: "Move",
		Shard:     args.Shard,
		GID:       args.GID,
	}
	err, _ := sc.executeCommand(op)
	reply.Err = err
	reply.WrongLeader = err == ErrWrongLeader
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		RequestId: nrand(),
		MessageId: args.MessageId,
		Operation: "Query",
		ConfigNum: args.Num,
	}
	err, cfg := sc.executeCommand(op)
	reply.Err = err
	reply.Config = cfg
	reply.WrongLeader = err == ErrWrongLeader
}

func (sc *ShardCtrler) executeCommand(op Op) (Err, Config) {

	_, _, isLeader := sc.Raft().Start(op)
	if !isLeader {
		return ErrWrongLeader, Config{}
	}
	sc.mu.Lock()
	ch := make(chan Config, 1)
	sc.opChan[op.RequestId] = ch
	sc.mu.Unlock()
	select {
	case <-time.After(500 * time.Millisecond):
		return ErrTimeout, Config{}
	case cfg := <-ch:
		return OK, cfg
	}
}

func (sc *ShardCtrler) reBalance(groups map[int][]string, shards [NShards]int) [NShards]int {
	newShards := [NShards]int{}
	if len(groups) == 0 {
		return newShards
	}
	if len(groups) == 1 {
		for idx, _ := range newShards {
			for gid, _ := range groups {
				newShards[idx] = gid
			}
		}
		return newShards
	}
	groupCollect := make(map[int][]int)
	var sortedGroup []int
	initGid := -1
	for gid, _ := range groups {
		sortedGroup = append(sortedGroup, gid)
		var sd []int
		groupCollect[gid] = sd
		if gid > initGid {
			initGid = gid
		}
	}
	for idx, gid := range shards {
		newShards[idx] = gid
		if _, ok := groupCollect[gid]; ok {
			groupCollect[gid] = append(groupCollect[gid], idx)
		} else {
			newShards[idx] = initGid
			groupCollect[initGid] = append(groupCollect[initGid], idx)
		}
	}
	sort.Ints(sortedGroup)
	for {
		min := 257
		max := 0
		maxGid := -1
		minGid := -1
		for _, gid := range sortedGroup {
			arr := groupCollect[gid]
			if len(arr) > max {
				max = len(arr)
				maxGid = gid
			}
			if len(arr) < min {
				min = len(arr)
				minGid = gid
			}
		}
		if max-min > 1 {
			maxArr := groupCollect[maxGid]
			exchangeIndex := maxArr[0]
			maxArr = maxArr[1:]
			groupCollect[maxGid] = maxArr
			newShards[exchangeIndex] = minGid
			groupCollect[minGid] = append(groupCollect[minGid], exchangeIndex)
		} else {
			break
		}
	}
	return newShards
}

func (sc *ShardCtrler) doJoin(Servers map[int][]string) {
	lastedConfig := sc.lastedConfig()
	groups, shards := lastedConfig.deepCopy()
	var newGIDs []int
	for gid, servers := range Servers {
		if _, ok := groups[gid]; !ok {
			newGIDs = append(newGIDs, gid)
		}
		nServers := make([]string, len(servers))
		copy(nServers, servers)
		groups[gid] = nServers
	}
	sc.configs = append(sc.configs, Config{
		Num:    len(sc.configs),
		Shards: sc.reBalance(groups, shards),
		Groups: groups,
	})
}

func (sc *ShardCtrler) doLeave(GIDs []int) {
	lastedConfig := sc.lastedConfig()
	groups, shard := lastedConfig.deepCopy()
	for _, GID := range GIDs {
		delete(groups, GID)
	}
	sc.configs = append(sc.configs, Config{
		Num:    len(sc.configs),
		Shards: sc.reBalance(groups, shard),
		Groups: groups,
	})
}

func (sc *ShardCtrler) doMove(Shard int, GID int) {
	lastedConfig := sc.lastedConfig()
	groups, shards := lastedConfig.deepCopy()
	shards[Shard] = GID
	sc.configs = append(sc.configs, Config{
		Num:    len(sc.configs),
		Shards: shards,
		Groups: groups,
	})
}

func (sc *ShardCtrler) applier() {
	for {
		select {
		case <-sc.killedChan:
			return
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				op := msg.Command.(Op)
				sc.mu.Lock()
				configNum := 0
				if "Query" == op.Operation {
					if op.ConfigNum == -1 || op.ConfigNum >= len(sc.configs) {
						configNum = len(sc.configs) - 1
					} else {
						configNum = op.ConfigNum
					}
				} else if lastMessage, ok := sc.LastCommand[op.RequestId]; !ok || lastMessage < op.MessageId {
					switch op.Operation {
					case "Join":
						sc.doJoin(op.Servers)
					case "Leave":
						sc.doLeave(op.GIDs)
					case "Move":
						sc.doMove(op.Shard, op.GID)
					default:
						panic("unknown operation")
					}
					sc.LastCommand[op.RequestId] = op.MessageId
				}
				if ch, ok := sc.opChan[op.RequestId]; ok {
					ch <- sc.configs[configNum]
				}
				sc.mu.Unlock()
			}
		}
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	sc.killedChan <- struct{}{}
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.opChan = make(map[int64]chan Config)
	sc.LastCommand = make(map[int64]int64)
	sc.killedChan = make(chan struct{})

	go sc.applier()

	return sc
}
