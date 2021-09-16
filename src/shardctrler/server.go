package shardctrler


import (
	"6.824/raft"
	"log"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const (
	Debug = false
	//Debug = true
	CONSENSUS_TIMEOUT = 5000 // ms

	QueryOp = "query"
	JoinOp = "join"
	LeaveOp = "leave"
	MoveOp = "move"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	maxraftstate int
	applyCh chan raft.ApplyMsg

	// Your data here.
	// TODO First Use ArrayIndex as the "Num" , Guess Test Will not Skip Query Num
  	configs []Config // indexed by config num

	//configNumIndex map[int]int // config(nun) -> configs Array Indexs
	waitApplyCh map[int]chan Op // index(raft) -> chan
	lastRequestId map[int64]int // clientid -> requestID

	// last SnapShot point , raftIndex
	lastSSPointRaftLogIndex int
}


type Op struct {
	// Your data here.
	Operation string
	ClientId int64
	RequestId int
	Num_Query int
	Servers_Join  map[int][]string
	Gids_Leave []int
	Shard_Move int
	Gid_Move int
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.maxraftstate = -1


	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	sc.waitApplyCh = make(map[int]chan Op)
	sc.lastRequestId = make(map[int64]int)

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0{
		sc.ReadSnapShotToInstall(snapshot)
	}
	go sc.ReadRaftApplyCommandLoop()
	return sc
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	//DPrintf("[Join]Server %d ,From Client %d, Request %d, groupNum %d", sc.me,args.ClientId,args.Requestid,len(args.Servers))
	if _,ifLeader := sc.rf.GetState(); !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//DPrintf("=============JOIN Info===============")
	//for gid, servers := range args.Servers {
	//	DPrintf("[Gid]===%d", gid)
	//	DPrintf("[servers]%v",servers)
	//}
	op := Op{Operation: JoinOp, ClientId: args.ClientId, RequestId: args.Requestid, Servers_Join: args.Servers}
	raftIndex,_,_ := sc.rf.Start(op)

	// create WaitForCh
	sc.mu.Lock()
	chForRaftIndex, exist := sc.waitApplyCh[raftIndex]
	if !exist {
		sc.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = sc.waitApplyCh[raftIndex]
	}
	sc.mu.Unlock()

	//Timeout WaitFor
	select {
	case <- time.After(time.Millisecond*CONSENSUS_TIMEOUT):
		if sc.ifRequestDuplicate(op.ClientId, op.RequestId){
			reply.Err = OK
		} else {
			DPrintf("[JOIN TIMEOUT]From Client %d (Request %d) To Server %d, raftIndex %d",args.ClientId,args.Requestid, sc.me, raftIndex)
			reply.Err = ErrWrongLeader
		}
	case raftCommitOp := <-chForRaftIndex:
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}

	sc.mu.Lock()
	delete(sc.waitApplyCh, raftIndex)
	sc.mu.Unlock()
	return

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	//DPrintf("[Leave]Server %d, From Client %d, Request %d, leaveGidNum %d", sc.me,args.ClientId,args.Requestid,args.GIDs)
	if _,ifLeader := sc.rf.GetState(); !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Operation: LeaveOp, ClientId: args.ClientId, RequestId: args.Requestid, Gids_Leave: args.GIDs}
	raftIndex,_,_ := sc.rf.Start(op)

	// create WaitForCh
	sc.mu.Lock()
	chForRaftIndex, exist := sc.waitApplyCh[raftIndex]
	if !exist {
		sc.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = sc.waitApplyCh[raftIndex]
	}
	sc.mu.Unlock()

	//Timeout WaitFor
	select {
	case <- time.After(time.Millisecond*CONSENSUS_TIMEOUT):
		DPrintf("[Leave TIMEOUT]From Client %d (Request %d) To Server %d, raftIndex %d",args.ClientId,args.Requestid, sc.me, raftIndex)
		if sc.ifRequestDuplicate(op.ClientId, op.RequestId){
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case raftCommitOp := <-chForRaftIndex:
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			reply.Err = OK
		}else {
			reply.Err = ErrWrongLeader
		}
	}

	sc.mu.Lock()
	delete(sc.waitApplyCh, raftIndex)
	sc.mu.Unlock()
	return
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	//DPrintf("[Move]Server %d, From Client %d, Request %d, Shard %d to Gid %d", sc.me,args.ClientId,args.Requestid,args.Shard, args.GID)
	if _,ifLeader := sc.rf.GetState(); !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Operation: MoveOp, ClientId: args.ClientId, RequestId: args.Requestid, Shard_Move: args.Shard,Gid_Move: args.GID}
	raftIndex,_,_ := sc.rf.Start(op)

	// create WaitForCh
	sc.mu.Lock()
	chForRaftIndex, exist := sc.waitApplyCh[raftIndex]
	if !exist {
		sc.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = sc.waitApplyCh[raftIndex]
	}
	sc.mu.Unlock()

	//Timeout WaitFor
	select {
	case <- time.After(time.Millisecond*CONSENSUS_TIMEOUT):
		DPrintf("[Move TIMEOUT]From Client %d (Request %d) To Server %d, raftIndex %d",args.ClientId,args.Requestid, sc.me, raftIndex)
		if sc.ifRequestDuplicate(op.ClientId, op.RequestId){
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case raftCommitOp := <-chForRaftIndex:
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}

	sc.mu.Lock()
	delete(sc.waitApplyCh, raftIndex)
	sc.mu.Unlock()
	return
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	//DPrintf("[Query]Server %d,From Client %d, Request %d, Num %d", sc.me,args.ClientId,args.Requestid,args.Num)
	if _,ifLeader := sc.rf.GetState(); !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//DPrintf("[Leader]Server %d is the leader",sc.me)
	op := Op{Operation: QueryOp,ClientId: args.ClientId, RequestId: args.Requestid, Num_Query: args.Num}
	raftIndex,_,_ := sc.rf.Start(op)

	// create WaitForCh
	sc.mu.Lock()
	chForRaftIndex, exist := sc.waitApplyCh[raftIndex]
	if !exist {
		sc.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = sc.waitApplyCh[raftIndex]
	}
	sc.mu.Unlock()

	//Timeout WaitFor
	select {
	case <- time.After(time.Millisecond*CONSENSUS_TIMEOUT):
		DPrintf("[Query TIMEOUT]From Client %d (Request %d) To Server %d, raftIndex %d",args.ClientId,args.Requestid, sc.me, raftIndex)
		_,ifLeader := sc.rf.GetState()
		if sc.ifRequestDuplicate(op.ClientId, op.RequestId) && ifLeader{
			reply.Config = sc.ExecQueryOnController(op)
			reply.Err = OK
		}else {
			reply.Err = ErrWrongLeader
		}
	case raftCommitOp := <-chForRaftIndex:
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			// Exec Query
			reply.Config = sc.ExecQueryOnController(op)
			reply.Err = OK
		}
	}

	sc.mu.Lock()
	delete(sc.waitApplyCh, raftIndex)
	sc.mu.Unlock()
	return
}


//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}


