package shardkv


import (
	"6.824/labrpc"
	"log"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"
import "6.824/shardctrler"

const (
	Debug = false
	//Debug = true
	CONSENSUS_TIMEOUT = 500 // ms
	CONFIGCHECK_TIMEOUT = 90
	SENDSHARDS_TIMEOUT = 150
	NShards = shardctrler.NShards

	GETOp = "get"
	PUTOp = "put"
	APPENDOp = "append"
	MIGRATESHARDOp = "migrate"
	NEWCONFIGOp = "newconfig"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string // "get" "put" "append"
	Key string
	Value string
	ClientId int64
	RequestId int
	Config_NEWCONFIG shardctrler.Config
	MigrateData_MIGRATE []ShardComponent
	ConfigNum_MIGRATE int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	dead 		 int32
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int // Use config.Shards ==> server contians which shards
	ctrlers      []*labrpc.ClientEnd
	mck       	 *shardctrler.Clerk
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvDB []ShardComponent // every Shard has it's independent data
	waitApplyCh map[int]chan Op // index(raft) -> chan
	// lastRequestId map[int64]int // clientid -> requestID

	// last SnapShot point , raftIndex
	lastSSPointRaftLogIndex int

	config shardctrler.Config
	migratingShard [NShards]bool
	//allContainShard [NShards]bool

}

func (kv *ShardKV) DprintfKVDB(){
	//
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	DPrintf("[=======KVDBINFO======]")
	DPrintf("[Who]Gid %d, server %d",kv.gid,kv.me)
	DPrintf("[isMigRating]%v",kv.migratingShard)
	DPrintf("[kvDB]%v",kv.kvDB)
	DPrintf("[Shards]%v",kv.config.Shards)
}

// return (ifResponsible, ifAvailble)
func (kv *ShardKV) CheckShardState(clientNum int,shardIndex int)(bool,bool){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.config.Num == clientNum && kv.config.Shards[shardIndex] == kv.gid, !kv.migratingShard[shardIndex]
}

func (kv *ShardKV) CheckMigrateState(shardComponets []ShardComponent) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _,shdata := range shardComponets {
		if kv.migratingShard[shdata.ShardIndex] {
			return false
		}
	}
	return true
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	shardIndex := key2shard(args.Key)
	ifRespons, ifAvali := kv.CheckShardState(args.ConfigNum,shardIndex)
	if !ifRespons {
		reply.Err = ErrWrongGroup
		return
	}
	if !ifAvali {
		reply.Err = ErrWrongLeader
		return
	}

	//DPrintf("[GET SendToWrongLeader]From Client %d (Request %d) To Server %d",args.ClientId,args.RequestId, kv.me)
	op := Op{Operation: GETOp, Key: args.Key, Value: "", ClientId: args.ClientId, RequestId: args.RequestId}

	raftIndex, _, _ := kv.rf.Start(op)
	//DPrintf("[GET StartToRaft]From Client %d (Request %d) To Server %d, key %v, raftIndex %d", args.ClientId, args.RequestId, kv.me, op.Key, raftIndex)

	// create waitForCh
	kv.mu.Lock()
	chForRaftIndex, exist := kv.waitApplyCh[raftIndex]
	if !exist {
		kv.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = kv.waitApplyCh[raftIndex]
	}
	kv.mu.Unlock()
	// timeout
	select {
	case <-time.After(time.Millisecond * CONSENSUS_TIMEOUT):
		//DPrintf("[GET TIMEOUT!!!]From Client %d (Request %d) To Server %d, key %v, raftIndex %d", args.ClientId, args.RequestId, kv.me, op.Key, raftIndex)

		_, ifLeaderr := kv.rf.GetState()
		if kv.ifRequestDuplicate(op.ClientId, op.RequestId, key2shard(op.Key)) && ifLeaderr {
			valueg, exists := kv.ExecuteGetOpOnKVDB(op)
			if exists {
				reply.Err = OK
				reply.Value = valueg
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}

	case raftCommitOp := <-chForRaftIndex:
		//DPrintf("[WaitChanGetRaftApplyMessage<--]Server %d , get Command <-- Index:%d , ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v", kv.me, raftIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value)
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			valuep, existt := kv.ExecuteGetOpOnKVDB(op)
			if existt {
				reply.Err = OK
				reply.Value = valuep
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}

	}
	kv.mu.Lock()
	delete(kv.waitApplyCh, raftIndex)
	kv.mu.Unlock()
	return

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	shardIndex := key2shard(args.Key)
	ifRespons, ifAvali := kv.CheckShardState(args.ConfigNum,shardIndex)
	//DPrintf("[!!!!!!!]%v,%v,%d,%d",ifRespons,ifAvali,kv.config.Num,args.ConfigNum)
	if !ifRespons {
		reply.Err = ErrWrongGroup
		return
	}
	if !ifAvali {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Operation: args.Opreation, Key: args.Key, Value: args.Value, ClientId: args.ClientId, RequestId: args.RequestId}

	raftIndex, _, _ := kv.rf.Start(op)
	//DPrintf("[PUTAPPEND StartToRaft]From Client %d (Request %d) To Server %d, key %v, raftIndex %d",args.ClientId,args.RequestId, kv.me, op.Key, raftIndex)

	// create waitForCh
	kv.mu.Lock()
	chForRaftIndex, exist := kv.waitApplyCh[raftIndex]
	if !exist {
		kv.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = kv.waitApplyCh[raftIndex]
	}
	kv.mu.Unlock()

	select {
	case <- time.After(time.Millisecond*CONSENSUS_TIMEOUT) :
		//DPrintf("[TIMEOUT PUTAPPEND !!!!]Server %d , get Command <-- Index:%d , ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v",kv.me, raftIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value)
		if kv.ifRequestDuplicate(op.ClientId,op.RequestId,key2shard(op.Key)){
			reply.Err = OK
		} else{
			reply.Err = ErrWrongLeader
		}

	case raftCommitOp := <- chForRaftIndex :
		//DPrintf("[WaitChanGetRaftApplyMessage<--]Server %d , get Command <-- Index:%d , ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v",kv.me, raftIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value)
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId  {
			reply.Err = OK
		}else{
			reply.Err = ErrWrongLeader
		}

	}
	kv.mu.Lock()
	delete(kv.waitApplyCh,raftIndex)
	kv.mu.Unlock()
	return
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead,1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}


//
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
//
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
	kv.kvDB = make([]ShardComponent, NShards)
	for shard:=0;shard<NShards;shard++ {
		kv.kvDB[shard] = ShardComponent{ShardIndex: shard, KVDBOfShard: make(map[string]string), ClientRequestId: make(map[int64]int)}
	}

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.waitApplyCh = make(map[int]chan Op)

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0{
		kv.ReadSnapShotToInstall(snapshot)
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.ReadRaftApplyCommandLoop()
	go kv.PullNewConfigLoop()
	go kv.SendShardToOtherGroupLoop()

	return kv
}
