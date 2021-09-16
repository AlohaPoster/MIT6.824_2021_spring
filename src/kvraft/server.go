package kvraft

import (
	//"2021_lab3/src/labgob"
	//"2021_lab3/src/labrpc"
	// original
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"time"

	// only for test
	// "2021_lab3/src/raft"
	"log"
	"sync"
	"sync/atomic"
)

const (
	Debug = false
	//Debug = true
	CONSENSUS_TIMEOUT = 500 // ms
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
	// reciver from applyCh , the entries that raft applied
	Operation string // "get" "put" "append"
	Key string
	Value string
	ClientId int64
	RequestId int
	// IfDuplicate bool // Duplicate command can't be applied twice , but only for PUT and APPEND
}


type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvDB map[string]string
	waitApplyCh map[int]chan Op // index(raft) -> chan
	lastRequestId map[int64]int // clientid -> requestID

	// last SnapShot point , raftIndex
	lastSSPointRaftLogIndex int
}

func (kv *KVServer) DprintfKVDB(){
	if !Debug {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for key,value := range kv.kvDB {
		DPrintf("[DBInfo ----]Key : %v, Value : %v",key,value)
	}
}


// RPC Handler for request from clerk
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	//DPrintf("[GET Request get]From Client %d (Request %d) To Server %d",args.ClientId,args.RequestId, kv.me)
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		//DPrintf("[GET SendToWrongLeader]From Client %d (Request %d) To Server %d",args.ClientId,args.RequestId, kv.me)
		return
	}

	op := Op{Operation: "get", Key: args.Key, Value: "", ClientId: args.ClientId, RequestId: args.RequestId}

	raftIndex, _, _ := kv.rf.Start(op)
	DPrintf("[GET StartToRaft]From Client %d (Request %d) To Server %d, key %v, raftIndex %d",args.ClientId,args.RequestId, kv.me, op.Key, raftIndex)

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
	case <- time.After(time.Millisecond*CONSENSUS_TIMEOUT) :
		DPrintf("[GET TIMEOUT!!!]From Client %d (Request %d) To Server %d, key %v, raftIndex %d",args.ClientId,args.RequestId, kv.me, op.Key, raftIndex)

		_,ifLeader := kv.rf.GetState()
		if kv.ifRequestDuplicate(op.ClientId, op.RequestId) && ifLeader{
			value, exist := kv.ExecuteGetOpOnKVDB(op)
			if exist {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}

		case raftCommitOp := <-chForRaftIndex:
			DPrintf("[WaitChanGetRaftApplyMessage<--]Server %d , get Command <-- Index:%d , ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v",kv.me, raftIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value)
			if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId  {
				value, exist := kv.ExecuteGetOpOnKVDB(op)
				if exist {
					reply.Err = OK
					reply.Value = value
				} else {
					reply.Err = ErrNoKey
					reply.Value = ""
				}
			} else{
				reply.Err = ErrWrongLeader
			}

	}

	kv.mu.Lock()
	delete(kv.waitApplyCh, raftIndex)
	kv.mu.Unlock()
	return

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//DPrintf("[PUTAPPEND Request get]From Client %d (Request %d) To Server %d",args.ClientId,args.RequestId, kv.me)
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		//DPrintf("[PUTAPPEND SendToWrongLeader]From Client %d (Request %d) To Server %d",args.ClientId,args.RequestId, kv.me)
		return
	}

	op := Op{Operation: args.Opreation, Key: args.Key, Value: args.Value, ClientId: args.ClientId, RequestId: args.RequestId}

	raftIndex, _, _ := kv.rf.Start(op)
	DPrintf("[PUTAPPEND StartToRaft]From Client %d (Request %d) To Server %d, key %v, raftIndex %d",args.ClientId,args.RequestId, kv.me, op.Key, raftIndex)

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
			DPrintf("[TIMEOUT PUTAPPEND !!!!]Server %d , get Command <-- Index:%d , ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v",kv.me, raftIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value)
			if kv.ifRequestDuplicate(op.ClientId,op.RequestId){
				reply.Err = OK
			} else{
				reply.Err = ErrWrongLeader
			}

		case raftCommitOp := <- chForRaftIndex :
			DPrintf("[WaitChanGetRaftApplyMessage<--]Server %d , get Command <-- Index:%d , ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v",kv.me, raftIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value)
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
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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
	DPrintf("[InitKVServer---]Server %d",me)
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvDB = make(map[string]string)
	kv.waitApplyCh = make(map[int]chan Op)
	kv.lastRequestId = make(map[int64]int)

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0{
		kv.ReadSnapShotToInstall(snapshot)
	}

	go kv.ReadRaftApplyCommandLoop()
	return kv
}
