package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Operation string
	Key       string
	Value     string
	MsgId     int64
	RequestId int64
	ClientId  int64
	Term      int
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type ApplyMsg struct {
	Value     string
	Error     Err
	ApplyTerm int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Your definitions here.
	servers     []*labrpc.ClientEnd
	Db          map[string]string
	opChan      map[int64]chan ApplyMsg
	LastApplied map[int64]int64
	killCh      chan struct{}
}

func (kv *KVServer) isRepeated(clientId int64, msgId int64) bool {
	if lastMsgId, ok := kv.LastApplied[clientId]; ok {
		return lastMsgId == msgId
	}
	return false
}

func (kv *KVServer) getData(key string) (err Err, value string) {
	if v, ok := kv.Db[key]; ok {
		err = OK
		value = v
		return
	} else {
		err = ErrNoKey
		return
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Operation: "Get",
		Key:       args.Key,
		ClientId:  args.ClientId,
		MsgId:     args.MsgId,
		RequestId: nrand(),
	}
	res := kv.executeCommand(op)
	reply.Err = res.Error
	reply.Value = res.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	op := Op{
		Operation: args.Op,
		Key:       args.Key,
		Value:     args.Value,
		MsgId:     args.MsgId,
		RequestId: nrand(),
		ClientId:  args.ClientId,
	}
	reply.Err = kv.executeCommand(op).Error
}

func (kv *KVServer) executeCommand(op Op) (res ApplyMsg) {
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

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	kv.killCh <- struct{}{}
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) readPersistent(snapshot []byte) {
	w := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(w)
	if d.Decode(&kv.Db) != nil || d.Decode(&kv.LastApplied) != nil {
		panic("decode err")
	}
}

func (kv *KVServer) applyLooper() {
	for !kv.killed() {
		select {
		case <-kv.killCh:
			return
		case msg := <-kv.applyCh:
			if msg.SnapshotValid {
				kv.mu.Lock()
				kv.readPersistent(msg.Snapshot)
				kv.mu.Unlock()
				continue
			}
			op := msg.Command.(Op)
			kv.mu.Lock()
			isRepeated := kv.isRepeated(op.ClientId, op.MsgId)
			switch op.Operation {
			case "Put":
				if !isRepeated {
					kv.Db[op.Key] = op.Value
					kv.LastApplied[op.ClientId] = op.MsgId
				}
			case "Append":
				if !isRepeated {
					_, v := kv.getData(op.Key)
					kv.Db[op.Key] = v + op.Value
					kv.LastApplied[op.ClientId] = op.MsgId
				}
			case "Get":
			default:
				panic("unknown operation")
			}
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				if e.Encode(kv.Db) != nil || e.Encode(kv.LastApplied) != nil {
					panic("encode err")
				}
				kv.rf.Snapshot(msg.CommandIndex, w.Bytes())
			}
			if ch, ok := kv.opChan[op.RequestId]; ok {
				_, value := kv.getData(op.Key)
				ch <- ApplyMsg{
					Error: OK,
					Value: value,
				}
			}
			kv.mu.Unlock()
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.servers = servers
	kv.Db = make(map[string]string)
	kv.opChan = make(map[int64]chan ApplyMsg)
	kv.LastApplied = make(map[int64]int64)
	kv.killCh = make(chan struct{})

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persister = persister
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	snapshot := kv.persister.ReadSnapshot()
	if snapshot != nil && len(snapshot) > 0 {
		kv.readPersistent(snapshot)
	}

	// You may need initialization code here.
	go kv.applyLooper()

	return kv
}
