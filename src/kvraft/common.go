package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	// ErrWrongConsensus = "ErrWrongConsensus"
	ErrServerKilled = "ErrServerKilled"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Opreation string // "Put" or "Append"
	ClientId int64
	RequestId int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
	// LeaderId int
}

type GetArgs struct {
	Key string
	ClientId int64
	RequestId int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	// LeaderId int
	Value string
}
