package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"log"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Index   int
	Command interface{}
	Term    int
}

const (
	Follower int = iota
	Candidate
	Leader
)

const MinRandomTimeout = 350
const MaxRandomTimeout = 500

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCond *sync.Cond
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//chans
	validHeartBeatCh  chan struct{}
	voteGrantedCh     chan struct{}
	majorGrantCh      chan struct{}
	demoteFollowerCh  chan struct{}
	broadcastCh       chan struct{}
	appliedCh         chan ApplyMsg
	installSnapshotCh chan struct{}
	killCh            chan struct{}

	// persistent state on all server
	currentTerm       int
	voteFor           int
	log               []LogEntry
	lastIncludedIndex int
	lastIncludedTerm  int

	// volatile state
	// all server
	state       int
	leaderId    int
	nVotes      int
	commitIndex int
	lastApplied int
	// just on leader
	nextIndex  []int
	matchIndex []int
}

type PersistentState struct {
	Term              int
	VoteFor           int
	LastIncludedIndex int
	LastIncludedTerm  int
	Log               []LogEntry
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) + rf.lastIncludedIndex
}

func (rf *Raft) lastLogTerm() int {
	return rf.logIndexer(rf.lastLogIndex()).Term
}

func (rf *Raft) logIndexer(logIndex int) LogEntry {
	if logIndex > rf.lastIncludedIndex+len(rf.log) {
		panic("ERROR: index greater than log length!\n")
	} else if logIndex < rf.lastIncludedIndex {
		panic("ERROR: index smaller than log snapshot!\n")
	} else if logIndex == rf.lastIncludedIndex {
		//fmt.Printf("WARNING: index == l.LastIncludedIndex\n")
		return LogEntry{Index: rf.lastIncludedIndex, Term: rf.lastIncludedTerm, Command: nil}
	}
	return rf.log[logIndex-rf.lastIncludedIndex-1]
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	//var term int
	//var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.state == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.persister.Save(rf.EncodeState(), rf.persister.ReadSnapshot())
}

func (rf *Raft) EncodeState() []byte {
	state := PersistentState{
		Term:              rf.currentTerm,
		VoteFor:           rf.voteFor,
		Log:               rf.log,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(state) != nil {
		log.Fatalf("encode error")
		return nil
	}
	data := w.Bytes()
	return data
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var state PersistentState
	if d.Decode(&state) != nil {
		log.Fatalf("Decode error")
		return
	}
	rf.currentTerm = state.Term
	rf.log = state.Log
	rf.voteFor = state.VoteFor
	rf.lastIncludedIndex = state.LastIncludedIndex
	rf.lastIncludedTerm = state.LastIncludedTerm
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.commitIndex
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	DPrintf("Snapshot Myself %v , %v", rf.me, index)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex {
		return
	}
	rf.log = append([]LogEntry(nil), rf.log[index-rf.lastIncludedIndex:]...)
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.logIndexer(index).Term
	rf.persister.Save(rf.EncodeState(), snapshot)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term             int
	Success          bool
	ConflictLogIndex int
	ConflictTerm     int
}

// InstallSnapshotArgs InstallSnapshot RPC arguments structure
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// InstallSnapshotReply InstallSnapshot RPC reply structure
type InstallSnapshotReply struct {
	Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if args.Term == rf.currentTerm {
		// already voted
		reply.Term = args.Term
		reply.VoteGranted = rf.voteFor == args.CandidateId
	} else {
		reply.Term = args.Term
		reply.VoteGranted = false
		rf.demoteToFollower(args.Term)
		if args.LastLogTerm > rf.lastLogTerm() || (args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex()) {
			reply.VoteGranted = true
			rf.voteFor = args.CandidateId
			rf.persist()
			notify(rf.voteGrantedCh, "voteGrantedCh")
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// heartbeat
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm || rf.state != Follower {
		rf.demoteToFollower(args.Term)
	}
	reply.ConflictLogIndex = -1
	reply.ConflictTerm = -1
	reply.Term = args.Term
	if args.PrevLogIndex < rf.lastIncludedIndex {
		//outdated AppendEntries
		//only happen due to unreliable network
		return
	}
	notify(rf.validHeartBeatCh, "validHeartBeatCh")
	//lab 2b
	if args.PrevLogIndex > 0 && (args.PrevLogIndex > rf.lastLogIndex() || rf.logIndexer(args.PrevLogIndex).Term != args.PrevLogTerm) {
		//Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
		reply.Success = false

		if args.PrevLogIndex > rf.lastLogIndex() {
			reply.ConflictLogIndex = rf.lastLogIndex() + 1
			reply.ConflictTerm = -1
			return
		}

		reply.ConflictTerm = rf.logIndexer(args.PrevLogIndex).Term
		conflictTermFirstIndex := args.PrevLogIndex
		for conflictTermFirstIndex >= rf.lastIncludedIndex && rf.logIndexer(conflictTermFirstIndex).Term == reply.ConflictTerm {
			conflictTermFirstIndex--
		}
		reply.ConflictLogIndex = conflictTermFirstIndex + 1

		return
	}

	reply.Success = true

	var i int
	for i = 0; i < len(args.Entries); i++ {
		//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		//here can't write "for i =range(args.Entries)",
		//since it will not increment i when i==len(args.Entries)-1
		//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		//it spent me 2 hours here to debug.
		if args.PrevLogIndex+i+1 > rf.lastLogIndex() {
			break
		}
		if args.Entries[i].Term == rf.logIndexer(args.PrevLogIndex+i+1).Term {
			continue
		}
		//If an existing entry conflicts with a new one
		//(same index but different terms),
		//delete the existing entry and all that follow it
		rf.log = rf.log[:args.PrevLogIndex+i+1-rf.lastIncludedIndex-1]
		rf.persist()
		break
	}

	for j := i; j < len(args.Entries); j++ {
		rf.log = append(rf.log, args.Entries[j])
	}
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.lastLogIndex()) //index of last new entry

		if rf.commitIndex > rf.lastApplied {
			rf.applyCond.Signal()
		}
	}
}

//Receiver implementation:
// 1.  Reply immediately if term < currentTerm
// 2.  Create new snapshot file if first chunk (offset is 0)
// 3.  Write data into snapshot file at given offset
// 4.  Reply and wait for more data chunks if done is false
// 5.  Save snapshot file, discard any existing or partial snapshotwith a smaller index
// 6.  If existing log entry has same index and term as snapshot’slast included entry, retain log entries following it and reply
// 7.  Discard the entire log
// 8.  Reset state machine using snapshot contents (and loadsnapshot’s cluster configuration)
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.demoteToFollower(args.Term)
	}
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}
	//  less than mine , compact myself later
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}
	if args.LastIncludedIndex > rf.lastLogIndex() {
		rf.log = make([]LogEntry, 0)
	} else {
		rf.log = append([]LogEntry(nil), rf.log[args.LastIncludedIndex-rf.lastIncludedIndex:]...)
	}
	rf.lastIncludedIndex, rf.lastIncludedTerm = args.LastIncludedIndex, args.LastIncludedTerm
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = rf.commitIndex

	rf.persister.Save(rf.EncodeState(), args.Data)
	notify(rf.installSnapshotCh, "installSnapshotCh")
	go func() {
		rf.appliedCh <- ApplyMsg{
			SnapshotValid: true,
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotTerm:  args.LastIncludedTerm,
			Snapshot:      args.Data,
		}
	}()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Collect Vote from %v to %v in %v,reply [%v,%v],collect,[%v]", args.CandidateId, server, args.Term, reply.VoteGranted, reply.Term, rf.nVotes)
	if reply.Term > rf.currentTerm {
		rf.demoteToFollower(reply.Term)
		return ok
	}

	if rf.currentTerm > reply.Term || !reply.VoteGranted {
		return ok
	}

	rf.nVotes += 1
	if rf.nVotes > len(rf.peers)/2 {
		notify(rf.majorGrantCh, "majorGrantCh")
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return
	}
	rf.valid(server, args, *reply)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.demoteToFollower(reply.Term)
		return
	}
	rf.nextIndex[server] = rf.lastLogIndex() + 1
	rf.matchIndex[server] = args.LastIncludedIndex
}

func (rf *Raft) valid(server int, args *AppendEntriesArgs, reply AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.demoteToFollower(reply.Term)
		return
	}

	if rf.state != Leader {
		return
	}

	if reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		rf.adjustLeaderCommit()
		return
	}

	if reply.ConflictTerm == -1 {
		//follower's log shorter than rf.nextIndex[server]
		rf.nextIndex[server] = reply.ConflictLogIndex
	} else if reply.ConflictTerm < args.PrevLogTerm {
		//go back to last term of the leader
		for rf.nextIndex[server] > rf.lastIncludedIndex+1 && rf.logIndexer(rf.nextIndex[server]-1).Term == args.PrevLogTerm {
			rf.nextIndex[server]--
		}
		if rf.nextIndex[server] != 1 && rf.nextIndex[server] == rf.lastIncludedIndex+1 && rf.lastIncludedTerm == args.PrevLogTerm {
			//special case:
			//we are hitting the snapshot.
			rf.nextIndex[server]--
		}
	} else {
		//reply.ConflictEntryTerm > args.PrevLogTerm
		//go back to last term of the follower
		rf.nextIndex[server] = reply.ConflictLogIndex
	}
	DPrintf("rf.nextIndex[server] decreased: %d\n", rf.nextIndex[server])
}

func (rf *Raft) adjustFollowerCommit(leaderCommitIndex int, newEntryIndex int) {
	if leaderCommitIndex > rf.commitIndex {
		// If leaderCommit > commitIndex, set commitIndex =min(leaderCommit, index of last new entry)
		rf.commitIndex = Min(leaderCommitIndex, newEntryIndex)
		if rf.commitIndex > rf.lastApplied {
			rf.applyCond.Signal()
		}
	}
}

func (rf *Raft) adjustLeaderCommit() {

	for i := rf.lastLogIndex(); i > rf.lastIncludedIndex && rf.logIndexer(i).Term == rf.currentTerm; i-- {

		statics := 0
		for _, mi := range rf.matchIndex {
			if mi >= i {
				statics++
			}
		}
		if statics > len(rf.peers)/2 {
			rf.commitIndex = i
			if rf.commitIndex > rf.lastApplied {
				rf.applyCond.Signal()
			}
			break
		}

	}

}

func (rf *Raft) RandomTimeout() time.Duration {
	return time.Duration(rand.Intn(MaxRandomTimeout-MinRandomTimeout)+MinRandomTimeout) * time.Millisecond
}

func (rf *Raft) StartElection() {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("··········starting election [%v , %v , %v]·········", rf.me, rf.state, rf.currentTerm)
	// start election become candidate and increment termId
	rf.state = Candidate
	rf.currentTerm += 1
	rf.voteFor = rf.me
	rf.nVotes = 1
	rf.persist()
	requestVoteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}
	for node := range rf.peers {
		if rf.me != node {
			voteReply := RequestVoteReply{}
			go rf.sendRequestVote(node, &requestVoteArgs, &voteReply)
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.state == Leader
	if !isLeader {
		return index, rf.currentTerm, isLeader
	}
	rf.log = append(rf.log, LogEntry{
		Index:   rf.lastLogIndex() + 1,
		Term:    rf.currentTerm,
		Command: command,
	})
	rf.persist()
	rf.matchIndex[rf.me] = rf.lastLogIndex()
	go rf.broadcast()
	notify(rf.broadcastCh, "broadcastCh")
	return rf.lastLogIndex(), rf.lastLogTerm(), isLeader
}

func (rf *Raft) broadcast() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return
	}
	for node := range rf.peers {
		if rf.me != node {
			prevLogIndex := rf.nextIndex[node] - 1
			if prevLogIndex < rf.lastIncludedIndex {
				args := &InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.leaderId,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					Data:              rf.persister.ReadSnapshot(),
				}
				reply := &InstallSnapshotReply{}
				go rf.sendInstallSnapshot(node, args, reply)
			} else {
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  rf.logIndexer(prevLogIndex).Term,
					Entries:      rf.log[prevLogIndex-rf.lastIncludedIndex:],
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesReply{}
				go rf.sendAppendEntries(node, &args, &reply)
			}
		}
	}
}

func (rf *Raft) promoteToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Follower {
		return
	}
	rf.state = Leader
	for node := range rf.peers {
		rf.nextIndex[node] = rf.lastLogIndex() + 1
		rf.matchIndex[node] = 0
	}
	rf.matchIndex[rf.me] = rf.lastLogIndex()
	go rf.broadcast()
}

func (rf *Raft) demoteToFollower(term int) {
	rf.state = Follower
	rf.nVotes = 0
	rf.voteFor = -1
	rf.currentTerm = term
	notify(rf.demoteFollowerCh, "demoteFollowerCh")
	rf.persist()
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		state := rf.state
		currentTerm := rf.currentTerm
		rf.mu.Unlock()
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		DPrintf("ticker loop , me : %v ; state : %v ; term : %v", rf.me, state, currentTerm)
		// time.Sleep().
		switch state {
		case Follower:
			select {
			case <-time.After(rf.RandomTimeout()):
				rf.StartElection()
			case <-rf.validHeartBeatCh:
				DPrintf("Follower %v get validHeartBeatCh", rf.me)
			case <-rf.voteGrantedCh:
				// reset timeout do nothing
			case <-rf.installSnapshotCh:
			case <-rf.killCh:
			}
		case Candidate:
			select {
			case <-time.After(rf.RandomTimeout()):
				// start another term election
				rf.StartElection()
			case <-rf.majorGrantCh:
				DPrintf("Candidate %v become leader", rf.me)
				rf.promoteToLeader()
			case <-rf.demoteFollowerCh:
				// another leader has elected do nothing
			case <-rf.killCh:
			}
		case Leader:
			select {
			case <-time.After(100 * time.Millisecond):
				rf.broadcast()
			case <-rf.demoteFollowerCh:
				DPrintf("leader failed")
			case <-rf.broadcastCh:

			case <-rf.killCh:

			}
		}
	}
}

func notify(eventCh chan struct{}, eventType string) {
	// From: https://stackoverflow.com/questions/25657207/how-to-know-a-buffered-channel-is-full
	select {
	case eventCh <- struct{}{}:
		DPrintf("Notified a %v event!\n", eventType)
	default:
		// Do nothing; nothing was waiting on an event
	}
}

func (rf *Raft) entryLogApplier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logIndexer(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
				CommandTerm:  rf.logIndexer(rf.lastApplied).Term,
			}
			rf.mu.Unlock()
			//IMPORTANT: must **not** holding the lock while sending to applyCh.
			//OR will cause deadlock(In 2D, since Snapshot() need to hold rf.mu)!
			rf.appliedCh <- msg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.nVotes = 0
	rf.voteFor = -1
	rf.state = Follower
	rf.currentTerm = 0
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.validHeartBeatCh = make(chan struct{})
	rf.voteGrantedCh = make(chan struct{})
	rf.majorGrantCh = make(chan struct{})
	rf.demoteFollowerCh = make(chan struct{})
	rf.broadcastCh = make(chan struct{})
	rf.killCh = make(chan struct{})
	rf.appliedCh = applyCh
	rf.installSnapshotCh = make(chan struct{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.entryLogApplier()

	return rf
}
