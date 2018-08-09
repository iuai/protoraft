package raft

func (rf *Raft) getLastLogIndex() int {
	return maxInt(rf.log[len(rf.log)-1].Index, rf.lastIncludedIndex)
}

func (rf *Raft) getLastStoredLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getFirstStoredLogIndex() int {
	if len(rf.log) > 1 {
		return rf.log[1].Index
	} else {
		return 0
	}
}

func (rf *Raft) getLastLogTerm() int {
	return maxInt(rf.log[len(rf.log)-1].Term, rf.lastIncludedTerm)
}

func (rf *Raft) getLogByIndex(index int) LogEntry {
	return rf.log[rf.getSliceIndex(index)]
}

func (rf *Raft) getSliceIndex(index int) int {
	return index - rf.lastIncludedIndex
}
