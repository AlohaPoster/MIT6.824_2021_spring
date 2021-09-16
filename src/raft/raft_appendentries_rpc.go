package raft

import "time"

// Append Entries RPC structure
type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	ConflictingIndex int // optimizer func for find the nextIndex
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[GetHeartBeat]Sever %d, from Leader %d(term %d), mylastindex %d, leader.preindex %d",rf.me,args.LeaderId,args.Term,rf.getLastIndex(),args.PrevLogIndex)

	// rule 1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictingIndex = -1
		return
	}

	rf.currentTerm = args.Term
	// rf.leaderId = args.LeaderId
	reply.Term = rf.currentTerm
	reply.Success = true
	reply.ConflictingIndex = -1

	if rf.state!=FOLLOWER{
		rf.changeState(TO_FOLLOWER,true)
	}else{
		rf.lastResetElectionTime = time.Now()
		rf.persist()
	}

	// confilict
	// for my snapshot until 15,and len 3, you give me 10 - 20,I give you 16
	if rf.lastSSPointIndex > args.PrevLogIndex{
		reply.Success = false
		reply.ConflictingIndex = rf.getLastIndex() + 1
		return
	}

	if rf.getLastIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.ConflictingIndex = rf.getLastIndex()
		DPrintf("[AppendEntries ERROR1]Sever %d ,prevLogIndex %d,Term %d, rf.getLastIndex %d, rf.getLastTerm %d, Confilicing %d",rf.me,args.PrevLogIndex,args.PrevLogTerm,rf.getLastIndex(),rf.getLastTerm(),reply.ConflictingIndex)
		return
	} else {
		if rf.getLogTermWithIndex(args.PrevLogIndex) != args.PrevLogTerm{
			reply.Success = false
			tempTerm := rf.getLogTermWithIndex(args.PrevLogIndex)
			for index := args.PrevLogIndex;index >= rf.lastSSPointIndex;index--{
				if rf.getLogTermWithIndex(index) != tempTerm{
					reply.ConflictingIndex = index+1
					DPrintf("[AppendEntries ERROR2]Sever %d ,prevLogIndex %d,Term %d, rf.getLastIndex %d, rf.getLastTerm %d, Confilicing %d",rf.me,args.PrevLogIndex,args.PrevLogTerm,rf.getLastIndex(),rf.getLastTerm(),reply.ConflictingIndex)
					break
				}
			}
			return
		}
	}

	//rule 3 & rule 4
	rf.log = append(rf.log[:args.PrevLogIndex+1-rf.lastSSPointIndex],args.Entries...)
	rf.persist()
	//if len(args.Entries) != 0{
	//	rf.printLogsForDebug()
	//}

	//rule 5
	if args.LeaderCommit > rf.commitIndex {
		rf.updateCommitIndex(FOLLOWER, args.LeaderCommit)
	}
	DPrintf("[FinishHeartBeat]Server %d, from leader %d(term %d), me.lastIndex %d",rf.me,args.LeaderId,args.Term,rf.getLastIndex())
	return
}

func (rf *Raft) leaderAppendEntries(){
	// send to every server to replicate logs to them
	for index := range rf.peers{
		if index == rf.me {
			continue
		}
		// parallel replicate logs to sever

		go func(server int){
			rf.mu.Lock()
			if rf.state!=LEADER{
				rf.mu.Unlock()
				return
			}
			prevLogIndextemp := rf.nextIndex[server]-1
			// DPrintf("[IfNeedSendSnapShot] leader %d ,lastSSPIndex %d, server %d ,prevIndex %d",rf.me,rf.lastSSPointIndex,server,prevLogIndextemp)
			if prevLogIndextemp < rf.lastSSPointIndex{
				go rf.leaderSendSnapShot(server)
				rf.mu.Unlock()
				return
			}

			aeArgs := AppendEntriesArgs{}

			if rf.getLastIndex() >= rf.nextIndex[server] {
				DPrintf("[LeaderAppendEntries]Leader %d (term %d) to server %d, index %d --- %d",rf.me,rf.currentTerm,server,rf.nextIndex[server],rf.getLastIndex())
				//rf.printLogsForDebug()
				entriesNeeded := make([]Entry,0)
				entriesNeeded = append(entriesNeeded,rf.log[rf.nextIndex[server]-rf.lastSSPointIndex:]...)
				prevLogIndex,prevLogTerm := rf.getPrevLogInfo(server)
				aeArgs = AppendEntriesArgs{
					rf.currentTerm,
					rf.me,
					prevLogIndex,
					prevLogTerm,
					entriesNeeded,
					rf.commitIndex,
				}
			}else {
				prevLogIndex,prevLogTerm := rf.getPrevLogInfo(server)
				aeArgs = AppendEntriesArgs{
					rf.currentTerm,
					rf.me,
					prevLogIndex,
					prevLogTerm,
					[] Entry{},
					rf.commitIndex,
				}
				DPrintf("[LeaderSendHeartBeat]Leader %d (term %d) to server %d,nextIndex %d, matchIndex %d, lastIndex %d",rf.me,rf.currentTerm,server,rf.nextIndex[server],rf.matchIndex[server],rf.getLastIndex())
			}
			aeReply := AppendEntriesReply{}
			rf.mu.Unlock()

			re := rf.sendAppendEntries(server,&aeArgs,&aeReply)

			//if re == false{
			//	rf.mu.Lock()
			//	DPrintf("[HeartBeat ERROR]Leader %d (term %d) get no reply from server %d",rf.me,rf.currentTerm,server)
			//	rf.mu.Unlock()
			//}

			if re == true {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state!=LEADER{
					return
				}

				if aeReply.Term > rf.currentTerm {
					rf.currentTerm = aeReply.Term
					rf.changeState(TO_FOLLOWER,true)
					return
				}

				DPrintf("[HeartBeatGetReturn] Leader %d (term %d) ,from Server %d, prevLogIndex %d",rf.me,rf.currentTerm,server,aeArgs.PrevLogIndex)

				if aeReply.Success {
					DPrintf("[HeartBeat SUCCESS] Leader %d (term %d) ,from Server %d, prevLogIndex %d",rf.me,rf.currentTerm,server,aeArgs.PrevLogIndex)
					rf.matchIndex[server] = aeArgs.PrevLogIndex + len(aeArgs.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					rf.updateCommitIndex(LEADER, 0)
				}

				if !aeReply.Success {
					if aeReply.ConflictingIndex!= -1 {
						DPrintf("[HeartBeat CONFLICT] Leader %d (term %d) ,from Server %d, prevLogIndex %d, Confilicting %d",rf.me,rf.currentTerm,server,aeArgs.PrevLogIndex,aeReply.ConflictingIndex)
						rf.nextIndex[server] = aeReply.ConflictingIndex
					}
				}
			}

		}(index)

	}
}

func (rf *Raft) leaderAppendEntriesTicker() {
	for rf.killed() == false {
		time.Sleep(HEARTBEAT_TIMEOUT * time.Millisecond)
		rf.mu.Lock()
		if rf.state == LEADER {
			rf.mu.Unlock()
			rf.leaderAppendEntries()
		} else {
			rf.mu.Unlock()
		}
	}
}