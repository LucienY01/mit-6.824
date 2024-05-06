package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	get = iota
	putAppend
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type          int
	GetArgs       GetArgs
	PutAppendArgs PutAppendArgs
}

type KVServer struct {
	mu      sync.Mutex // lock order: first kv then rf
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data       map[string]string
	cond       *sync.Cond
	applyIndex int
	term       int
	isLeader   bool

	latestOpID    map[int64]int64 // map[clientID]latestOpID
	persister     *raft.Persister
	snapshotIndex int
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh

		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)

			kv.mu.Lock()
			if op.Type == putAppend && kv.latestOpID[op.PutAppendArgs.ClientID] < op.PutAppendArgs.OpID {
				args := &op.PutAppendArgs
				switch args.Op {
				case "Put":
					kv.data[args.Key] = args.Value

				case "Append":
					v, ok := kv.data[args.Key]
					if !ok {
						kv.data[args.Key] = args.Value
					} else {
						v += args.Value
						kv.data[args.Key] = v
					}
				}

				kv.latestOpID[args.ClientID] = args.OpID
				DPrintf("server %d: apply a write op id=%d, %s(%s, %s) from client %d",
					kv.me, args.OpID, args.Op, args.Key, args.Value, args.ClientID)

			} else if args := &op.GetArgs; op.Type == get && kv.latestOpID[args.ClientID] < args.OpID {
				kv.latestOpID[args.ClientID] = args.OpID
				DPrintf("server %d: apply a get op id=%d, get(%s) from client %d",
					kv.me, args.OpID, args.Key, args.ClientID)
			} else {
				if op.Type == putAppend {
					DPrintf("%d skip putAppend op %d from client %d", kv.me, op.PutAppendArgs.OpID, op.PutAppendArgs.ClientID)
				} else {
					DPrintf("%d skip get op %d from client %d", kv.me, op.GetArgs.OpID, op.GetArgs.ClientID)
				}
			}

			kv.applyIndex = applyMsg.CommandIndex

			// We need to check whether raft's term has changed before waking up a handler,
			// since periodic termChecker could be unaware of change of term.
			// If the server is unaware of change of term, advance in applyIndex or latestOpID
			// can result in a successful but invalid request response.
			term, _ := kv.rf.GetState()
			kv.term = term

			kv.cond.Broadcast()
			kv.mu.Unlock()
		} else if applyMsg.SnapshotValid {
			if kv.maxraftstate == -1 {
				panic("should not use snapshot")
			}

			kv.mu.Lock()
			DPrintf("server %d receives a snapshot from RPC, snapshot: term=%d, index=%d",
				kv.me, applyMsg.SnapshotTerm, applyMsg.SnapshotIndex)
			ok := kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot)
			if ok {
				DPrintf("server %d: condInstall ok, starts to decode snapshot, snapshot: term=%d, index=%d",
					kv.me, applyMsg.SnapshotTerm, applyMsg.SnapshotIndex)
				kv.decodeSnapshot(applyMsg.Snapshot)
				// kv.applyIndex has changed, we need to wake up handlers waiting for kv.cond.
				//
				// We need to check whether raft's term has changed before waking up a handler,
				// since periodic termChecker could be unaware of change of term.
				// If the server is unaware of change of term, advance in applyIndex or latestOpID
				// can result in a successful but invalid request response.
				term, _ := kv.rf.GetState()
				kv.term = term
				kv.cond.Broadcast()
			}
			kv.mu.Unlock()
		} else {
			panic("invalid applyMsg")
		}
	}
}

func (kv *KVServer) termChecker(originalTerm int) {
	checkPeriod := raft.MinTimeout / 10 * time.Millisecond
	for !kv.killed() {
		time.Sleep(checkPeriod)

		kv.mu.Lock()
		curTerm, isLeader := kv.rf.GetState()
		DPrintf("server %d: termChecker: curTerm=%d, isLeader=%v", kv.me, curTerm, isLeader)
		if curTerm > originalTerm {
			DPrintf("server %d: termChecker: term change: %d -> %d", kv.me, originalTerm, curTerm)
			kv.term = curTerm
			kv.cond.Broadcast()
			if isLeader {
				originalTerm = curTerm
			} else {
				kv.isLeader = false
				kv.mu.Unlock()
				return
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) handleRPC(args arguments) Err {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		DPrintf("server %d is not leader, return", kv.me)
		return ErrWrongLeader
	}
	latestOpID := kv.latestOpID[args.getClientID()]
	if args.getOpID() <= latestOpID {
		return OK
	}

	// Start agreement on the unexecuted operation.
	var op Op
	switch v := args.(type) {
	case *GetArgs:
		op = Op{
			Type:    get,
			GetArgs: *v,
		}
	case *PutAppendArgs:
		op = Op{
			Type:          putAppend,
			PutAppendArgs: *v,
		}
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("server %d is not leader, return at rf.Start()", kv.me)
		return ErrWrongLeader
	}

	// If it knows for the first time that it becomes leader, start a termChecker.
	if !kv.isLeader {
		DPrintf("leader %d: term %d -> %d, go termChecker", kv.me, kv.term, term)
		kv.term = term
		kv.isLeader = true
		go kv.termChecker(term)
	}

	for kv.term <= term && kv.applyIndex < index {
		kv.cond.Wait()
	}

	if kv.term > term {
		// Raft's term has changed. The server may have lost its leadership.
		return ErrWrongLeader
	}

	return OK
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("server %d: start handle get op id=%d, get(%s) from client %d", kv.me, args.OpID, args.Key, args.ClientID)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	err := kv.handleRPC(args)
	if err == ErrWrongLeader {
		reply.Err = err
		return
	}

	v, ok := kv.data[args.Key]
	if !ok {
		reply.Err = ErrNoKey
	} else {
		reply.Err = OK
		reply.Value = v
	}
	DPrintf("leader %d: finish handle get op id=%d, get(%s) from client %d", kv.me, args.OpID, args.Key, args.ClientID)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("server %d: start handle write op id=%d, %s(%s, %s) from client %d",
		kv.me, args.OpID, args.Op, args.Key, args.Value, args.ClientID)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	err := kv.handleRPC(args)
	if err == ErrWrongLeader {
		reply.Err = err
		return
	}

	reply.Err = OK
	DPrintf("leader %d: finish handle write op id=%d, %s(%s, %s) from client %d",
		kv.me, args.OpID, args.Op, args.Key, args.Value, args.ClientID)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) snapshotTaker() {
	for !kv.killed() {
		time.Sleep(10 * time.Millisecond)

		kv.mu.Lock()
		if kv.applyIndex <= kv.snapshotIndex {
			kv.mu.Unlock()
			continue
		}

		stateSize := kv.persister.RaftStateSize()
		DPrintf("server %d: state size = %d, maxraftstate = %d, applyIndex=%d, snapshotIndex=%d",
			kv.me, stateSize, kv.maxraftstate, kv.applyIndex, kv.snapshotIndex)
		if stateSize > kv.maxraftstate {
			DPrintf("server %d starts to take a snapshot, new snapshotIndex=%d",
				kv.me, kv.applyIndex)
			snapshot := kv.encodeSnapshot()
			kv.rf.Snapshot(kv.applyIndex, snapshot)
			kv.snapshotIndex = kv.applyIndex
		}
		kv.mu.Unlock()
	}
}

type Snapshot struct {
	Data       map[string]string
	LatestOpID map[int64]int64
	ApplyIndex int
}

func (kv *KVServer) encodeSnapshot() []byte {
	snapshot := Snapshot{
		Data:       kv.data,
		LatestOpID: kv.latestOpID,
		ApplyIndex: kv.applyIndex,
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(snapshot); err != nil {
		panic(err)
	}
	return w.Bytes()
}

func (kv *KVServer) decodeSnapshot(snapshotBytes []byte) {
	snapshot := Snapshot{}
	r := bytes.NewBuffer(snapshotBytes)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&snapshot); err != nil {
		panic(err)
	}
	kv.data = snapshot.Data
	kv.latestOpID = snapshot.LatestOpID
	kv.applyIndex = snapshot.ApplyIndex
	kv.snapshotIndex = snapshot.ApplyIndex
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	DPrintf("start kv server %d", me)
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Snapshot{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	if maxraftstate == -1 {
		// do not use snapshot
		kv.data = make(map[string]string)
		kv.latestOpID = make(map[int64]int64)
	} else {
		snapshot := persister.ReadSnapshot()
		if len(snapshot) == 0 {
			// The server has not taken any snapshot yet.
			kv.data = make(map[string]string)
			kv.latestOpID = make(map[int64]int64)
		} else {
			// recover from snapshot
			kv.decodeSnapshot(snapshot)
			DPrintf("server %d recover from a snapshot, snapshotIndex=%d", me, kv.snapshotIndex)
		}
	}
	kv.persister = persister
	kv.cond = sync.NewCond(&kv.mu)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applier()
	if maxraftstate != -1 {
		go kv.snapshotTaker()
	}

	return kv
}
