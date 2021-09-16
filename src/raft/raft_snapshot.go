package raft

import "time"

type InstallSnapshotArgs struct{
	Term int
	LeaderId int
	LastIncludeIndex int
	LastIncludeTerm int
	Data[] byte
	//Done bool
}

type InstallSnapshotReply struct {
	Term int
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.lastSSPointIndex >= index || index > rf.commitIndex{
		return
	}
	// snapshot the entrier form 1:index(global)
	tempLog := make([]Entry,0)
	tempLog = append(tempLog,Entry{})

	for i := index+1;i<=rf.getLastIndex();i++ {
		tempLog = append(tempLog,rf.getLogWithIndex(i))
	}

	// TODO fix it in lab 4
	if index == rf.getLastIndex()+1 {
		rf.lastSSPointTerm = rf.getLastTerm()
	}else {
		rf.lastSSPointTerm = rf.getLogTermWithIndex(index)
	}

	rf.lastSSPointIndex = index

	rf.log = tempLog
	if index > rf.commitIndex{
		rf.commitIndex = index
	}
	if index > rf.lastApplied{
		rf.lastApplied = index
	}
	DPrintf("[SnapShot]Server %d sanpshot until index %d, term %d, loglen %d",rf.me,index,rf.lastSSPointTerm,len(rf.log)-1)
	rf.persister.SaveStateAndSnapshot(rf.persistData(),snapshot)
}

// InstallSnapShot RPC Handler
func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply){
	rf.mu.Lock()
	// DPrintf("[lock] sever %d get the lock",rf.me)
	if rf.currentTerm > args.Term{
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	reply.Term = args.Term
	if rf.state != FOLLOWER {
		rf.changeState(TO_FOLLOWER,true)
	}else{
		rf.lastResetElectionTime = time.Now()
		rf.persist()
	}

	if rf.lastSSPointIndex >= args.LastIncludeIndex{
		DPrintf("[HaveSnapShot] sever %d , lastSSPindex %d, leader's lastIncludeIndex %d",rf.me,rf.lastSSPointIndex,args.LastIncludeIndex)
		rf.mu.Unlock()
		return
	}

	index := args.LastIncludeIndex
	tempLog := make([]Entry,0)
	tempLog = append(tempLog,Entry{})

	for i := index+1;i<=rf.getLastIndex();i++ {
		tempLog = append(tempLog,rf.getLogWithIndex(i))
	}

	rf.lastSSPointTerm = args.LastIncludeTerm
	rf.lastSSPointIndex = args.LastIncludeIndex

	rf.log = tempLog
	if index > rf.commitIndex{
		rf.commitIndex = index
	}
	if index > rf.lastApplied{
		rf.lastApplied = index
	}
	rf.persister.SaveStateAndSnapshot(rf.persistData(),args.Data)
	//rf.persist()

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot: args.Data,
		SnapshotTerm: rf.lastSSPointTerm,
		SnapshotIndex: rf.lastSSPointIndex,
	}
	rf.mu.Unlock()

	rf.applyCh <- msg
	DPrintf("[FollowerInstallSnapShot]server %d installsnapshot from leader %d, index %d",rf.me,args.LeaderId,args.LastIncludeIndex)

}


func (rf *Raft) leaderSendSnapShot(server int){
	rf.mu.Lock()
	DPrintf("[LeaderSendSnapShot]Leader %d (term %d) send snapshot to server %d, index %d",rf.me,rf.currentTerm,server,rf.lastSSPointIndex)
	ssArgs := InstallSnapshotArgs{
		rf.currentTerm,
		rf.me,
		rf.lastSSPointIndex,
		rf.lastSSPointTerm,
		rf.persister.ReadSnapshot(),
	}
	ssReply := InstallSnapshotReply{}
	rf.mu.Unlock()

	re := rf.sendSnapShot(server,&ssArgs,&ssReply)

	if !re {
		DPrintf("[InstallSnapShot ERROR] Leader %d don't recive from %d",rf.me,server)
	}
	if re == true {
		rf.mu.Lock()
		if rf.state!=LEADER || rf.currentTerm!=ssArgs.Term{
			rf.mu.Unlock()
			return
		}
		if ssReply.Term > rf.currentTerm{
			rf.changeState(FOLLOWER,true)
			rf.mu.Unlock()
			return
		}

		DPrintf("[InstallSnapShot SUCCESS] Leader %d from sever %d",rf.me,server)
		rf.matchIndex[server] = ssArgs.LastIncludeIndex
		rf.nextIndex[server] = ssArgs.LastIncludeIndex + 1

		rf.mu.Unlock()
		return
	}
}

