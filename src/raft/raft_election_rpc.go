package raft

import "time"

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//rule 1 ------------
	if args.Term < rf.currentTerm {
		DPrintf("[ElectionReject++++++]Server %d reject %d, MYterm %d, candidate term %d",rf.me,args.CandidateId,rf.currentTerm,args.Term)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm {
		DPrintf("[ElectionToFollower++++++]Server %d(term %d) into follower,candidate %d(term %d) ",rf.me,rf.currentTerm,args.CandidateId,args.Term)
		rf.currentTerm = args.Term
		rf.changeState(TO_FOLLOWER,false)
		rf.persist()
	}

	//rule 2 ------------
	if rf.UpToDate(args.LastLogIndex,args.LastLogTerm) == false {
		DPrintf("[ElectionReject+++++++]Server %d reject %d, UpToDate",rf.me,args.CandidateId)
		//rf.printLogsForDebug()
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// arg.Term == rf.currentTerm
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId && args.Term == reply.Term {
		DPrintf("[ElectionReject+++++++]Server %d reject %d, Have voter for %d",rf.me,args.CandidateId,rf.votedFor)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}else {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.lastResetElectionTime = time.Now()
		rf.persist()
		DPrintf("[ElectionSUCCESS+++++++]Server %d voted for %d!",rf.me,args.CandidateId)
		return
	}

	//if args.Term == rf.currentTerm {
	//	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
	//		reply.VoteGranted = false
	//		reply.Term = rf.currentTerm
	//		return
	//	}else {
	//		// TODO : have vote for you ? what should I do? vote you again???
	//		//DPrintf("%d vote for %d!",rf.me,args.CandidateId)
	//		rf.votedFor = args.CandidateId
	//		reply.VoteGranted = true
	//		reply.Term = rf.currentTerm
	//		rf.lastResetElectionTime = time.Now()
	//		rf.persist()
	//		return
	//	}
	//
	//}

	return

}


func (rf *Raft) candidateJoinElection(){
	DPrintf("[JoinElection+++++++] Sever %d, term %d",rf.me,rf.currentTerm)
	for index := range rf.peers{
		if index == rf.me{
			continue
		}

		go func(server int){
			rf.mu.Lock()
			rvArgs := RequestVoteArgs{
				rf.currentTerm,
				rf.me,
				rf.getLastIndex(),
				rf.getLastTerm(),
			}
			rvReply := RequestVoteReply{}
			rf.mu.Unlock()
			// waiting code should free lock first.
			re := rf.sendRequestVote(server,&rvArgs,&rvReply)
			if re == true {
				rf.mu.Lock()
				if rf.state != CANDIDATE || rvArgs.Term < rf.currentTerm{
					rf.mu.Unlock()
					return
				}

				if rvReply.VoteGranted == true && rf.currentTerm == rvArgs.Term{
					rf.getVoteNum += 1
					if rf.getVoteNum >= len(rf.peers)/2+1 {
						DPrintf("[LeaderSuccess+++++] %d got votenum: %d, needed >= %d, become leader for term %d",rf.me,rf.getVoteNum,len(rf.peers)/2+1,rf.currentTerm)
						rf.changeState(TO_LEADER,true)
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					return
				}

				if rvReply.Term > rvArgs.Term {
					if rf.currentTerm < rvReply.Term{
						rf.currentTerm = rvReply.Term
					}
					rf.changeState(TO_FOLLOWER,false)
					rf.mu.Unlock()
					return
				}

				rf.mu.Unlock()
				return
			}

		}(index)

	}

}

// The ticker go routine starts a new election if this peer hasn't received
// use candidateJoinElection()

func (rf *Raft) candidateElectionTicker() {
	for rf.killed() == false {
		nowTime := time.Now()
		timet := getRand(int64(rf.me))
		time.Sleep(time.Duration(timet)*time.Millisecond)
		rf.mu.Lock()
		if rf.lastResetElectionTime.Before(nowTime) && rf.state != LEADER{
			// DPrintf("[ElectionTrickerRand++++++]Server %d, term %d, randTime %d",rf.me,rf.currentTerm,timet)
			rf.changeState(TO_CANDIDATE,true)
		}
		rf.mu.Unlock()

	}
}