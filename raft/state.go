package raft

func (rf *Raft) convertToFollower(term int) {
	DPrintf("convert server %v from %v to Follower, term %v => %v", rf.me, rf.state, rf.currentTerm, term)
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = NotVoted
}

func (rf *Raft) convertToLeader() {
	DPrintf("convert server %v from %v to Leader, term %v", rf.me, rf.state, rf.currentTerm)
	rf.state = Leader
	dropAndSet(rf.leaderCh)

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := range rf.peers {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		if i != rf.me {
			rf.matchIndex[i] = 0
		} else {
			rf.matchIndex[i] = rf.getLastLogIndex()
		}
	}
}

func (rf *Raft) convertToCandidate() {
	DPrintf("convert server %v from %v to Candidate, term %v => %v", rf.me, rf.state, rf.currentTerm, rf.currentTerm+1)
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
}
