package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type arguments interface {
	getClientID() int64
	getOpID() int64
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	ClientID int64
	OpID     int64
}

func (args *PutAppendArgs) getClientID() int64 {
	return args.ClientID
}

func (args *PutAppendArgs) getOpID() int64 {
	return args.OpID
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	ClientID int64
	OpID     int64
}

func (args *GetArgs) getClientID() int64 {
	return args.ClientID
}

func (args *GetArgs) getOpID() int64 {
	return args.OpID
}

type GetReply struct {
	Err   Err
	Value string
}
