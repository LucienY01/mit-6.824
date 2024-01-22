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

	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const isDebugMode = false

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

	// States for leader election (part 2A).
	currentTerm int
	votedFor    int
	log         []logEntry
	commitIndex int
	condCommit  *sync.Cond
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	startOfTimeout               time.Time
	timeout                      time.Duration
	lastSendingAppendEntriesTime time.Time
}

type logEntry struct {
	Command interface{}
	Term    int
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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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

func (rf *Raft) asUpToDateOrLess(args *RequestVoteArgs) bool {
	n := len(rf.log)

	if args.LastLogTerm > rf.log[n-1].Term {
		return true
	} else if args.LastLogTerm == rf.log[n-1].Term {
		return args.LastLogIndex >= n-1
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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
	}

	if rf.votedFor == notVoted && rf.state == followerState && rf.asUpToDateOrLess(args) {
		rf.votedFor = args.CandidateID
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.resetTimer()
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
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
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	replyFalse := func() {
		reply.Term = rf.currentTerm
		reply.Success = false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if isDebugMode {
		log.Printf("peer %d receive AppendEntries: curTerm = %d", rf.me, rf.currentTerm)
	}
	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
	} else if args.Term < rf.currentTerm { // step 1
		replyFalse()
		return
	}

	// At this point, we know that args.Term == rf.currentTerm,
	if rf.state == candidateState {
		rf.convertToFollower()
		rf.changeTimeout()
	} else if rf.state == leaderState {
		panic(fmt.Sprintf("In term %d, a leader receive RPC from another leader.", rf.currentTerm))
	}

	// At this point, we are sure that this peer is a follower.
	rf.resetTimer()

	// step 2
	if args.PrevLogIndex > len(rf.log)-1 || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		replyFalse()
		return
	}

	// step 3
	i := 0
	base := args.PrevLogIndex + 1
	for ; i < len(args.Entries); i++ {
		if base+i == len(rf.log) {
			break
		}

		if rf.log[base+i].Term != args.Entries[i].Term {
			rf.log = rf.log[:base+i]
			break
		}
	}

	// step 4
	newEntreis := args.Entries[i:]
	rf.log = append(rf.log, newEntreis...)

	// step 5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		rf.condCommit.Signal()
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	entry := logEntry{
		Command: command,
		Term:    term,
	}
	rf.log = append(rf.log, entry)

	n := len(rf.log)
	index = n - 1

	// Send AppendEntries to replicate the entry.
	replicateCh := make(chan int)
	for server, nextI := range rf.nextIndex {
		if server == rf.me {
			continue
		}

		if n-1 < nextI {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			for {
				nextI := rf.nextIndex[server]
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: nextI - 1,
					PrevLogTerm:  rf.log[nextI-1].Term,
					Entries:      rf.log[nextI:],
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()
				reply := AppendEntriesReply{}

				ok := false
				for !ok {
					if rf.killed() {
						replicateCh <- 0
						return
					}
					ok = rf.sendAppendEntries(server, &args, &reply)
				}

				rf.mu.Lock()
				if rf.currentTerm > args.Term {
					rf.mu.Unlock()
					replicateCh <- 0
					return
				}

				// Process the RPC response.
				if reply.Success {
					newNextIndex := nextI + len(args.Entries)
					// This RPC could be delayed. If a later RPC has succeeded,
					// we don't want this goroutine to reset the nextIndex backward.
					if rf.nextIndex[server] > newNextIndex {
						rf.mu.Unlock()
						replicateCh <- 0
						return
					}

					rf.nextIndex[server] = newNextIndex
					rf.matchIndex[server] = newNextIndex - 1
					rf.mu.Unlock()
					replicateCh <- 1
					return

				} else {
					if reply.Term > rf.currentTerm {
						rf.updateTerm(reply.Term)
						rf.mu.Unlock()
						replicateCh <- 0
						return
					}

					// Fail because of log inconsistency.
					rf.nextIndex[server]--
				}
			}
		}(server)
	}

	// Use a goroutine to collect a majority of matchIndex[i].
	go func() {
		count := 1
		done := false
		for i := 0; i < len(rf.peers)-1; i++ {
			count += <-replicateCh

			if !done && count > len(rf.peers)/2 {
				done = true
				go func() {
					rf.mu.Lock()
					if index > rf.commitIndex && rf.log[index].Term == rf.currentTerm {
						rf.commitIndex = index
						rf.condCommit.Signal()
					}
					rf.mu.Unlock()
				}()
			}
		}
	}()

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
func (rf *Raft) heartbeatSender() {
	const heartbeatTime = 150 * time.Millisecond

	rf.mu.Lock()
	rf.lastSendingAppendEntriesTime = time.Now().Add(-heartbeatTime)
	rf.mu.Unlock()

	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != leaderState {
			rf.mu.Unlock()
			break
		}

		// If the peer has sent AppendEntries RPC recently, skip sending this heartbeat.
		if time.Since(rf.lastSendingAppendEntriesTime) <= heartbeatTime/2 {
			rf.mu.Unlock()
			continue
		}

		nLog := len(rf.log)
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: nLog - 1,
			PrevLogTerm:  rf.log[nLog-1].Term,
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()

		for server := range rf.peers {
			if server == rf.me {
				continue
			}

			go func(server int) {
				reply := AppendEntriesReply{}
				ok := false
				for !ok {
					if rf.killed() {
						return
					}
					ok = rf.sendAppendEntries(server, &args, &reply)
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.currentTerm > args.Term {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.updateTerm(reply.Term)
					return
				}

				rf.lastSendingAppendEntriesTime = time.Now()
			}(server)
		}

		time.Sleep(heartbeatTime)
	}
	if isDebugMode {
		fmt.Printf("%d: convert to follower, term = %d", rf.me, rf.currentTerm)
	}
}

func (rf *Raft) convertToCandidate() {
	rf.mu.Lock()
	rf.state = candidateState
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetTimer()
	rf.changeTimeout()

	npeers := len(rf.peers)

	lastLogIndex := len(rf.log) - 1
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  rf.log[lastLogIndex].Term,
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
			// RPC could be delayed. Check if it is delayed for too long.
			if rf.currentTerm > args.Term {
				// drop reply and return
				rf.mu.Unlock()
				voteCh <- 0
				return
			}

			if reply.Term > rf.currentTerm {
				rf.updateTerm(reply.Term)
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
			go rf.heartbeatSender()
		}()
	}
}

// unit: ms
const (
	minTimeout = 400
	maxTimeout = 800
)

// Returns a timeout between [minTimeout, maxTimeout] at random.
func getRandomTimeout() time.Duration {
	return time.Duration(rand.Intn(maxTimeout-minTimeout+1)+minTimeout) * time.Millisecond
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(20 * time.Millisecond)

		rf.mu.Lock()
		if rf.state != leaderState && rf.timeOut() {
			go rf.convertToCandidate()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) isLeader() bool {
	return rf.state == leaderState
}

func (rf *Raft) applier() {
	rf.condCommit.L.Lock()
	for {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.mu.Unlock()
			rf.condCommit.Wait()
			rf.mu.Lock()
		}

		rf.lastApplied++
		lastApplied := rf.lastApplied
		cmd := rf.log[rf.lastApplied].Command
		rf.mu.Unlock()

		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      cmd,
			CommandIndex: lastApplied,
		}

		if isDebugMode {
			rf.mu.Lock()
			if rf.isLeader() {
				log.Printf("leader %d commit cmd, at index: %d", rf.me, lastApplied)
			} else {
				log.Printf("peer %d commit cmd, at index: %d", rf.me, lastApplied)
			}
			rf.mu.Unlock()
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
	rf.applyCh = applyCh
	rf.votedFor = notVoted
	rf.log = make([]logEntry, 1, 10)
	rf.condCommit = sync.NewCond(&sync.Mutex{})
	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}

	rf.matchIndex = make([]int, len(peers))

	now := time.Now()
	rf.startOfTimeout = now
	rf.timeout = getRandomTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
