package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK" // TODO A Instance of "Err"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrConfigNum = "ErrConfigNum"
)

type Err string

type ShardComponent struct {
	ShardIndex int
	KVDBOfShard map[string]string
	ClientRequestId map[int64]int
}


type MigrateShardArgs struct {
	 MigrateData []ShardComponent
	 ConfigNum int
}

type MigrateShardReply struct {
	Err Err
	ConfigNum int
}


// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Opreation    string // "Put" or "Append"
	ClientId int64
	RequestId int
	ConfigNum int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	ClientId int64
	RequestId int
	ConfigNum int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
