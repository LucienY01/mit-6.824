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
	//	"bytes"

	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	followerState = iota
	candidateState
	leaderState
)

const notVoted = -1

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state   int
	applyCh chan ApplyMsg

	// persistent states
	currentTerm int
	votedFor    int
	log         []logEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	startOfTimeout        time.Time
	timeout               time.Duration // duration of timeout
	lastAppendEntriesTime []time.Time
	condCommit            *sync.Cond // condition variable on commitIndex and lastApplied
	snapshotIndex         int        // last included index in snapshot
	snapshotTerm          int        // last included term in snapshot
	isInstallingSnapshot  bool
	expectedSnapshotIndex int
}

type logEntry struct {
	Command interface{}
	Term    int
	Index   int
}

func (rf *Raft) realIndex(logicIndex int) int {
	return logicIndex - (rf.snapshotIndex + 1)
}

func (rf *Raft) logicIndex(realIndex int) int {
	return rf.snapshotIndex + 1 + realIndex
}

func (rf *Raft) firstIndex() int {
	if len(rf.log) > 0 {
		return rf.log[0].Index
	} else {
		return rf.snapshotIndex
	}
}

// Returns rf.snapshotIndex if it has no log or last index in its log.
func (rf *Raft) lastIndex() int {
	if len(rf.log) > 0 {
		return rf.log[len(rf.log)-1].Index
	} else {
		return rf.snapshotIndex
	}
}

// Returns rf.snapshotIndex if it has no log or last log term in its log.
func (rf *Raft) lastTerm() int {
	if len(rf.log) > 0 {
		return rf.log[len(rf.log)-1].Term
	} else {
		return rf.snapshotTerm
	}
}

func (rf *Raft) getEntry(logicIndex int) *logEntry {
	return &rf.log[rf.realIndex(logicIndex)]
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == leaderState
	rf.mu.Unlock()

	return term, isleader
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	return w.Bytes()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// Need to lock on rf.mu before calling this function.
func (rf *Raft) persist() {
	// Your code here (2C).
	state := rf.encodeState()
	rf.persister.SaveRaftState(state)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log = make([]logEntry, 10)
	var snapshotIndex int
	var snapshotTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil ||
		d.Decode(&snapshotIndex) != nil || d.Decode(&snapshotTerm) != nil {
		panic("read persistent state: decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.snapshotIndex = snapshotIndex
		rf.snapshotTerm = snapshotTerm
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() { rf.isInstallingSnapshot = false }()

	if lastIncludedIndex < rf.lastApplied {
		// snapshot is old, ignore it
		DPrintf("peer %d ignores an nold snapshot, lastIncludedIndex=%d, rf.lastApplied=%d\n",
			rf.me, lastIncludedIndex, rf.lastApplied)
		return false
	}
	DPrintf("peer %d install snapshot by RPC index=%d, term=%d", rf.me, lastIncludedIndex, lastIncludedTerm)

	// encode raft state
	rf.discardEntries(lastIncludedIndex + 1)
	rf.snapshotIndex = lastIncludedIndex
	rf.snapshotTerm = lastIncludedTerm
	raftState := rf.encodeState()

	rf.persister.SaveStateAndSnapshot(raftState, snapshot)

	if rf.lastApplied < lastIncludedIndex {
		rf.lastApplied = lastIncludedIndex
		if rf.commitIndex < lastIncludedIndex {
			rf.commitIndex = lastIncludedIndex
		}
	}
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// encode raft state
	DPrintf("peer %d get entry at index %d, snapshotIndex=%d", rf.me, index, rf.snapshotIndex)
	lastIncludedTerm := rf.getEntry(index).Term
	DPrintf(`peer %d takes a snapshot index=%d, term=%d
	entries before discarding: %v`, rf.me, index, lastIncludedTerm, rf.log)
	rf.discardEntries(index + 1)
	DPrintf(`peer %d: entries after discarding: %v`, rf.me, rf.log)
	rf.snapshotIndex = index
	rf.snapshotTerm = lastIncludedTerm
	raftState := rf.encodeState()

	rf.persister.SaveStateAndSnapshot(raftState, snapshot)
}

// Returns a new log that discards entries before index.
// Need to lock on rf.mu before calling this function.
func (rf *Raft) discardEntries(index int) {
	realIndex := rf.realIndex(index)
	n := len(rf.log)
	if n <= realIndex {
		DPrintf("peer %d discards all entries", rf.me)
		rf.log = make([]logEntry, 0, 10)
		return
	}

	newLog := make([]logEntry, n-realIndex)
	copy(newLog, rf.log[realIndex:n])
	rf.log = newLog
}

func (rf *Raft) ReadSnapshot() map[string]string {
	snapshot := rf.persister.ReadSnapshot()
	if len(snapshot) == 0 {
		return nil
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	var lastIncludedTerm int
	data := make(map[string]string)
	if d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil || d.Decode(&data) != nil {
		panic("read snapshot: decode error")
	} else {
		rf.snapshotIndex = lastIncludedIndex
		rf.snapshotTerm = lastIncludedTerm
		return data
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	} else if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
		rf.persist()
	}
	rf.resetTimer()

	if rf.isInstallingSnapshot && args.LastIncludedIndex <= rf.expectedSnapshotIndex {
		// This is a repeated or old snapshot, so we ignore it.
		// The safety of Raft guarantees that we do not need to check the term.
		rf.mu.Unlock()
		return
	}

	if rf.snapshotIndex < args.LastIncludedIndex {
		// This condition means that the snpashot in RPC covers at least one entry.

		// Reset state machine using snapshot contents.
		applyMsg := ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
		rf.expectedSnapshotIndex = args.LastIncludedIndex
		rf.isInstallingSnapshot = true
		rf.mu.Unlock()
		DPrintf("peer %d sends a snapshot to applyCh, snapshot: index=%d, term=%d",
			rf.me, args.LastIncludedIndex, args.LastIncludedTerm)
		rf.applyCh <- applyMsg
	} else {
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) lessUpToDate(term, index int) bool {
	lastEntry := rf.log[len(rf.log)-1]
	if term > lastEntry.Term {
		return true
	} else if term == lastEntry.Term {
		return index > lastEntry.Index
	}
	return false
}

func (rf *Raft) lessUpToDateOrEqually(term, index int) bool {
	if term > rf.lastTerm() {
		return true
	} else if term == rf.lastTerm() {
		return index >= rf.lastIndex()
	}
	return false
}

func (rf *Raft) convertToFollower() {
	rf.state = followerState
}

func (rf *Raft) updateTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = notVoted
	rf.convertToFollower()
}

func (rf *Raft) notVoted() bool {
	return rf.votedFor == notVoted
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	needPersist := false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
		needPersist = true
	}

	if (rf.notVoted() || rf.votedFor == args.CandidateID) &&
		rf.lessUpToDateOrEqually(args.LastLogTerm, args.LastLogIndex) {

		rf.resetTimer()
		if rf.notVoted() {
			DPrintf("peer %d votes for candidate %d, term=%d\n", rf.me, args.CandidateID, rf.currentTerm)
			rf.votedFor = args.CandidateID
			rf.persist()
		}
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if needPersist {
		rf.persist()
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// Results for optimization of fast backing up.
	ConflictTerm  int
	ConflictIndex int
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	needPersist := false
	defer func() {
		if needPersist {
			rf.persist()
		}
	}()

	if args.Term > rf.currentTerm {
		DPrintf("peer %d receive AppendEntries from leader %d: update term %d => %d",
			rf.me, args.LeaderID, rf.currentTerm, args.Term)
		rf.updateTerm(args.Term)
		needPersist = true
	} else if args.Term < rf.currentTerm { // step 1
		DPrintf("peer %d receive AppendEntries from leader %d: argsTerm=%d < curTerm=%d, ignore",
			rf.me, args.LeaderID, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// At this point, we know that args.Term == rf.currentTerm.
	reply.Term = rf.currentTerm

	if rf.state == candidateState {
		rf.convertToFollower()
		rf.changeTimeout()
	} else if rf.state == leaderState {
		panic(fmt.Sprintf("current term = %d, args.Term = %d, leader %d receives RPC from leader %d.",
			rf.currentTerm, args.Term, rf.me, args.LeaderID))
	}

	// At this point, we are sure that this peer is a follower.
	rf.resetTimer()

	// step 2
	lastIndex := rf.lastIndex()
	if len(args.Entries) > 0 {
		if args.PrevLogIndex > lastIndex {
			reply.Success = false
			reply.ConflictTerm = -1
			reply.ConflictIndex = lastIndex + 1
			DPrintf(`peer %d receive AppendEntries from leader %d: curTerm=%d, log inconsistency: shorter than index %d
	log: %v
	entries from leader: %v`,
				rf.me, args.LeaderID, rf.currentTerm, args.PrevLogIndex,
				rf.log,
				args.Entries)
			return
		}

		if args.PrevLogIndex == rf.snapshotIndex {
			prevTerm := rf.snapshotTerm
			if prevTerm != args.PrevLogTerm {
				panic("leader of higher term does not contain a commited entry")
			}
		} else if rf.snapshotIndex < args.PrevLogIndex {
			prevTerm := rf.getEntry(args.PrevLogIndex).Term
			if prevTerm != args.PrevLogTerm {
				reply.Success = false
				reply.ConflictTerm = prevTerm

				// Set ConflictIndex to the first index in its log whose entry has term equal to ConflictTerm.
				i := rf.realIndex(args.PrevLogIndex - 1)
				for i >= 0 && rf.log[i].Term == prevTerm {
					i--
				}
				reply.ConflictIndex = rf.logicIndex(i + 1)
				DPrintf(`peer %d receive AppendEntries from leader %d: curTerm=%d, log inconsistency: not matched
	log from first entry whose term equal to ConflictTerm: %v
	args.PrevLogIndex: %d
	entries from leader: %v`,
					rf.me, args.LeaderID, rf.currentTerm,
					rf.log[i+1:],
					args.PrevLogIndex,
					args.Entries)
				return
			}
		}
	} else {
		// Do not do fast backing up if the RPC is a heartbeat.
		if args.PrevLogIndex == rf.snapshotIndex {
			if args.PrevLogTerm != rf.snapshotTerm {
				return
			}
		} else if args.PrevLogIndex > rf.snapshotIndex &&
			(args.PrevLogIndex > lastIndex || rf.getEntry(args.PrevLogIndex).Term != args.PrevLogTerm) {
			return
		}
	}

	reply.Success = true

	// step 3
	i := 0
	base := rf.realIndex(args.PrevLogIndex + 1)
	for ; i < len(args.Entries); i++ {
		if base+i == len(rf.log) {
			break
		}
		if args.PrevLogIndex+1+i <= rf.snapshotIndex {
			continue
		}

		if rf.log[base+i].Term != args.Entries[i].Term {
			DPrintf(`peer %d: curTerm=%d, delete conflicting entries:
	conflict index: %d
	log before delete: %v`, rf.me, rf.currentTerm, base+i, rf.log)
			rf.log = rf.log[:base+i]
			break
		}
	}

	// step 4
	newEntreis := args.Entries[i:]
	if len(newEntreis) > 0 {
		needPersist = true
		DPrintf(`peer %d: curTerm=%d, append new entries:
	log before append: %v
	new entries: %v`, rf.me, rf.currentTerm, rf.log, newEntreis)
		rf.log = append(rf.log, newEntreis...)
	}

	// step 5
	// fmt.Printf("peer %d: args.LeaderCommit=%d, rf.commitIndex=%d\n", rf.me, args.LeaderCommit, rf.commitIndex)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))

		if rf.commitIndex > rf.lastApplied {
			rf.condCommit.Signal()
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Binary search.
// If present, return a real index at which the entry's term equals to key. Otherwise, return -1.
// end need to be a real index.
func (rf *Raft) searchForTerm(end int, key int) int {
	a := rf.log[:end]
	lo := 0
	hi := len(a) - 1
	for lo <= hi {
		// Key is in a[lo..hi] or not present.
		mid := lo + (hi-lo)/2
		if key < a[mid].Term {
			hi = mid - 1
		} else if key > a[mid].Term {
			lo = mid + 1
		} else {
			return mid
		}
	}
	return -1
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != leaderState {
		isLeader = false
		return index, term, isLeader
	}

	term = rf.currentTerm
	index = rf.lastIndex() + 1
	entry := logEntry{
		Command: command,
		Term:    term,
		Index:   index,
	}
	rf.log = append(rf.log, entry)
	rf.persist()
	rf.matchIndex[rf.me] = index

	for server, nextIndex := range rf.nextIndex {
		if server == rf.me {
			continue
		}

		if index < nextIndex {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			targetNextIndex := rf.nextIndex[server]
			if rf.currentTerm > term || rf.lastIndex() < targetNextIndex {
				rf.mu.Unlock()
				return
			}
			// 		DPrintf(`leader %d sends entries starting from index %d to peer %d, curTerm=%d
			// entries to send: %v
			// whole log: %v`,
			// 			rf.me, targetNextIndex, server, rf.currentTerm, rf.log[rf.realIndex(targetNextIndex):], rf.log)

			for {
				logicNextIndex := rf.nextIndex[server]
				for logicNextIndex <= rf.snapshotIndex {
					// the entry of next index is in the snapshot, need to install snapshot
					DPrintf(`leader %d does not contain entry at index %d, send InstallSnapshot RPC to peer %d
	log: %v
	snapshot: index=%d, term=%d`,
						rf.me, logicNextIndex, server,
						rf.log,
						rf.snapshotIndex, rf.snapshotTerm)

					args := InstallSnapshotArgs{
						Term:              rf.currentTerm,
						LeaderID:          rf.me,
						LastIncludedIndex: rf.snapshotIndex,
						LastIncludedTerm:  rf.snapshotTerm,
						Data:              rf.persister.ReadSnapshot(),
					}
					rf.mu.Unlock()
					reply := InstallSnapshotReply{}
					ok := rf.sendInstallSnapshot(server, &args, &reply)
					for !ok {
						if rf.killed() {
							return
						}

						rf.mu.Lock()
						if rf.currentTerm > term {
							rf.mu.Unlock()
							return
						}
						rf.mu.Unlock()

						ok = rf.sendInstallSnapshot(server, &args, &reply)
					}
					rf.mu.Lock()

					if reply.Term > rf.currentTerm {
						rf.updateTerm(reply.Term)
						rf.persist()
						rf.mu.Unlock()
						return
					}
					if rf.currentTerm > term {
						rf.mu.Unlock()
						return
					}

					newNextIndex := args.LastIncludedIndex + 1
					if rf.nextIndex[server] > newNextIndex {
						// avoid backing off nextIndex
						logicNextIndex = rf.nextIndex[server]
					} else {
						rf.nextIndex[server] = args.LastIncludedIndex + 1
						logicNextIndex = args.LastIncludedIndex + 1
					}
					// After the RPC suceeded, this leader may has taken a new snapshot,
					// so we need the loop to go back to recheck the condition here.
				}
				realNextIndex := rf.realIndex(logicNextIndex)

				entries := make([]logEntry, len(rf.log)-realNextIndex)
				copy(entries, rf.log[realNextIndex:])
				var prevLogIndex int
				var prevLogTerm int
				if realNextIndex == 0 {
					prevLogIndex = rf.snapshotIndex
					prevLogTerm = rf.snapshotTerm
				} else {
					prevLogIndex = logicNextIndex - 1
					prevLogTerm = rf.log[realNextIndex-1].Term
				}
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()
				reply := AppendEntriesReply{}

				ok := rf.sendAppendEntries(server, &args, &reply)
				for !ok {
					if rf.killed() {
						return
					}

					// RPC fails after a long time. We should re-check the invariants.
					rf.mu.Lock()
					if rf.currentTerm > term || rf.lastIndex() < rf.nextIndex[server] {
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()

					ok = rf.sendAppendEntries(server, &args, &reply)
				}

				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.updateTerm(reply.Term)
					rf.persist()
					rf.mu.Unlock()
					return
				}

				if rf.currentTerm > term || rf.lastIndex() < rf.nextIndex[server] {
					rf.mu.Unlock()
					return
				}

				newNextIndex := logicNextIndex + len(args.Entries)
				// Check to avoid backing up a completed nextIndex.
				if rf.nextIndex[server] >= newNextIndex {
					rf.mu.Unlock()
					return
				}

				// Process the RPC response.
				if reply.Success {
					rf.lastAppendEntriesTime[server] = time.Now()

					rf.nextIndex[server] = newNextIndex
					oldMatchIndex := rf.matchIndex[server]
					rf.matchIndex[server] = newNextIndex - 1

					// Search for a majority of replication of a new entry.
					for N := rf.matchIndex[server]; N > oldMatchIndex; N-- {
						realN := rf.realIndex(N)
						if N <= rf.commitIndex || rf.log[realN].Term != rf.currentTerm {
							continue
						}

						count := 0
						for _, matchIndex := range rf.matchIndex {
							if matchIndex >= N {
								count++
							}
						}
						if count > len(rf.peers)/2 {
							// There exists a majority of replication, so it commits entry at index N.
							rf.commitIndex = N
							rf.condCommit.Signal()
							break
						}
					}
					rf.mu.Unlock()
					return

				} else {
					// Fail because of log inconsistency.
					rf.lastAppendEntriesTime[server] = time.Now()

					if rf.nextIndex[server] < logicNextIndex {
						// Some thread has already found a smaller nextIndex, just use it to retry.
						continue
					}

					// Back up nextIndex with optimization and retry.
					if reply.ConflictTerm == -1 {
						rf.nextIndex[server] = reply.ConflictIndex
					} else {
						if args.PrevLogIndex <= rf.snapshotIndex {
							// This leader has dicarded old entries.
							// Install a snapshot in the next loop.
							rf.nextIndex[server] = reply.ConflictIndex
						} else {
							// Search its log for ConflictTerm.
							// binary search
							i := rf.searchForTerm(rf.realIndex(args.PrevLogIndex), reply.ConflictTerm)
							if i == -1 {
								// If it does not find an entry with that term,
								// it should set nextIndex = ConflictIndex.
								rf.nextIndex[server] = reply.ConflictIndex
							} else {
								// If it finds an entry in its log with that term,
								// it should set nextIndex to be
								// the one beyond the index of the last entry in that term in its log.
								i++
								for rf.log[i].Term == reply.ConflictTerm {
									i++
								}
								rf.nextIndex[server] = rf.logicIndex(i)
							}

							// linear search
							// i := args.PrevLogIndex - 1
							// for i >= 1 && rf.log[i].Term != reply.ConflictTerm {
							// 	i--
							// }
							// if i == 0 {
							// 	// If it does not find an entry with that term,
							// 	// it should set nextIndex = ConflictIndex.
							// 	rf.nextIndex[server] = reply.ConflictIndex
							// } else {
							// 	// If it finds an entry in its log with that term,
							// 	// it should set nextIndex to be
							// 	// the one beyond the index of the last entry in that term in its log.
							// 	rf.nextIndex[server] = i + 1
							// }
						}
					}
					// 				DPrintf(`leader %d: AppendEntries to peer %d failed because of log inconsistency
					// after fast backing up, nextIndex=%d:
					// log[:nextIndex]: %v
					// log[nextIndex:]: %v`, rf.me, server, rf.nextIndex[server], rf.log[:rf.realIndex(rf.nextIndex[server])], rf.log[rf.realIndex(rf.nextIndex[server]):])
				}
			}
		}(server)
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Returns whether this peer has timed out.
// Need to lock on rf.mu before calling this function.
func (rf *Raft) timeOut() bool {
	return time.Since(rf.startOfTimeout) >= rf.timeout
}

// Need to lock on rf.mu before calling this function.
func (rf *Raft) resetTimer() {
	rf.startOfTimeout = time.Now()
}

// Change timeout to a randomized new timeout.
// Need to lock on rf.mu before calling this function.
func (rf *Raft) changeTimeout() {
	rf.timeout = getRandomTimeout()
}

// Send initial empty AppendEntries RPCs to each server.
// And repeat during idle periods.
func (rf *Raft) heartbeatSender(server int, term int) {
	const heartbeatTime = 150 * time.Millisecond

	rf.mu.Lock()
	rf.lastAppendEntriesTime[server] = time.Now().Add(-heartbeatTime)
	rf.mu.Unlock()

	for !rf.killed() {
		rf.mu.Lock()
		if rf.currentTerm > term {
			rf.mu.Unlock()
			return
		}

		sleepTime := heartbeatTime
		// If the peer recently did not send AppendEntries RPC successfully, send a heartbeat.
		idleTime := time.Since(rf.lastAppendEntriesTime[server])
		if idleTime >= heartbeatTime {
			go func() {
				rf.mu.Lock()
				if rf.currentTerm > term {
					rf.mu.Unlock()
					return
				}

				prevLogIndex := rf.nextIndex[server] - 1
				var prevLogTerm int
				if prevLogIndex == rf.snapshotIndex {
					prevLogTerm = rf.snapshotTerm
				} else if prevLogIndex < rf.snapshotIndex {
					args := InstallSnapshotArgs{
						Term:              rf.currentTerm,
						LeaderID:          rf.me,
						LastIncludedIndex: rf.snapshotIndex,
						LastIncludedTerm:  rf.snapshotTerm,
						Data:              rf.persister.ReadSnapshot(),
					}
					rf.mu.Unlock()
					reply := InstallSnapshotReply{}
					ok := rf.sendInstallSnapshot(server, &args, &reply)
					for !ok {
						if rf.killed() {
							return
						}

						rf.mu.Lock()
						if rf.currentTerm > term {
							rf.mu.Unlock()
							return
						}
						rf.mu.Unlock()

						ok = rf.sendInstallSnapshot(server, &args, &reply)
					}
					rf.mu.Lock()

					if reply.Term > rf.currentTerm {
						rf.updateTerm(reply.Term)
						rf.persist()
						rf.mu.Unlock()
						return
					}
					if rf.currentTerm > term {
						rf.mu.Unlock()
						return
					}
					// Another thread has updated nextIndex and reset the timer.
					// Thus we can skip this heartbeat.
					newNextIndex := args.LastIncludedIndex + 1
					if rf.nextIndex[server] > newNextIndex {
						rf.mu.Unlock()
						return
					}

					rf.nextIndex[server] = newNextIndex
					prevLogIndex = args.LastIncludedIndex
					prevLogTerm = args.LastIncludedTerm
				} else {
					prevLogTerm = rf.getEntry(prevLogIndex).Term
				}

				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      nil,
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()

				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, &args, &reply)
				for !ok {
					if rf.killed() {
						return
					}

					// RPC fails after a long time. We should re-check the invariants.
					rf.mu.Lock()
					if rf.currentTerm > term || time.Since(rf.lastAppendEntriesTime[server]) < heartbeatTime/2 {
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()

					ok = rf.sendAppendEntries(server, &args, &reply)
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					rf.updateTerm(reply.Term)
					rf.persist()
					return
				}

				if rf.currentTerm > args.Term {
					return
				}

				// At this point, we know reply.Term == args.Term.
				// This means the follower consider the leader as valid and reset its timer.
				rf.lastAppendEntriesTime[server] = time.Now()
			}()
		} else {
			sleepTime = heartbeatTime - idleTime
		}
		rf.mu.Unlock()

		time.Sleep(sleepTime)
	}
}

func (rf *Raft) convertToCandidate(oldTerm int) {
	rf.mu.Lock()
	if rf.state == leaderState || !rf.timeOut() || rf.currentTerm > oldTerm {
		rf.mu.Unlock()
		return
	}

	rf.state = candidateState
	rf.currentTerm = oldTerm + 1
	DPrintf("peer %d votes for itself, term=%d\n", rf.me, rf.currentTerm)
	rf.votedFor = rf.me
	rf.persist()
	rf.resetTimer()
	rf.changeTimeout()

	npeers := len(rf.peers)

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.lastIndex(),
		LastLogTerm:  rf.lastTerm(),
	}
	rf.mu.Unlock()

	voteCh := make(chan int)
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go func(server int) {
			reply := RequestVoteReply{}
			ok := false
			for !ok {
				if rf.killed() {
					voteCh <- 0
					return
				}
				ok = rf.sendRequestVote(server, &args, &reply)
			}

			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.updateTerm(reply.Term)
				rf.persist()
				rf.mu.Unlock()
				voteCh <- 0
				return
			}

			// RPC could be delayed. Check if it is delayed for too long.
			if rf.currentTerm > args.Term {
				// drop reply and return
				rf.mu.Unlock()
				voteCh <- 0
				return
			}
			rf.mu.Unlock()

			if reply.VoteGranted {
				voteCh <- 1
			} else {
				voteCh <- 0
			}
		}(server)
	}

	votes := 1
	done := false // whether the peer has received votes from majority of servers.
	for i := 0; i < npeers-1; i++ {
		votes += <-voteCh

		if votes <= npeers/2 {
			continue
		}

		// The rest of the loop is used to drain voteCh.
		if done {
			continue
		}

		// Has received votes from a majority.
		// Starts to convert to leader.
		done = true
		go func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// RPC could be delayed. Check if it is delayed for too long.
			if rf.currentTerm > args.Term {
				return
			}

			// This peer may have already received a heartbeat from a new leader
			// and has become a follower.
			if rf.state == followerState {
				return
			}

			rf.state = leaderState
			nLog := rf.lastIndex() + 1
			for server := range rf.peers {
				rf.nextIndex[server] = nLog
				rf.matchIndex[server] = 0

				if server != rf.me {
					go rf.heartbeatSender(server, rf.currentTerm)
				}
			}

		}()
	}
}

// unit: ms
const (
	MinTimeout = 400
	MaxTimeout = 800
)

// Returns a timeout between [minTimeout, maxTimeout] at random.
func getRandomTimeout() time.Duration {
	return time.Duration(rand.Intn(MaxTimeout-MinTimeout+1)+MinTimeout) * time.Millisecond
}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(20 * time.Millisecond)

		rf.mu.Lock()
		if !rf.isLeader() && rf.timeOut() {
			go rf.convertToCandidate(rf.currentTerm)
		}
		rf.mu.Unlock()
	}
}

// Need to lock on rf.mu.
func (rf *Raft) isLeader() bool {
	return rf.state == leaderState
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	if rf.snapshotIndex > 0 {
		// restart with a snapshot
		rf.lastApplied = rf.snapshotIndex
		rf.commitIndex = rf.snapshotIndex
	}
	rf.mu.Unlock()

	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied ||
			(rf.isInstallingSnapshot && rf.lastApplied+1 <= rf.expectedSnapshotIndex) {

			rf.condCommit.Wait()
		}

		rf.lastApplied++
		lastApplied := rf.lastApplied
		realLastApplied := rf.realIndex(lastApplied)
		cmd := rf.log[realLastApplied].Command

		var role string
		if rf.isLeader() {
			role = "leader"
		} else {
			role = "peer"
		}
		DPrintf(role+" %d commits cmd=%d at index %d, curTerm=%d", rf.me, cmd, lastApplied, rf.currentTerm)
		rf.mu.Unlock()

		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      cmd,
			CommandIndex: lastApplied,
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	nPeers := len(peers)

	rf.applyCh = applyCh
	rf.votedFor = notVoted
	rf.log = make([]logEntry, 0, 10)
	rf.condCommit = sync.NewCond(&rf.mu)
	rf.lastApplied = 0
	rf.nextIndex = make([]int, nPeers)
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}

	rf.matchIndex = make([]int, nPeers)

	now := time.Now()
	rf.startOfTimeout = now
	rf.timeout = getRandomTimeout()
	rf.lastAppendEntriesTime = make([]time.Time, nPeers)
	rf.snapshotIndex = 0
	rf.snapshotTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
