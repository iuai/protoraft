package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()

	DPrintf("[raft] [AppendEntries] [server %v]: received AppendEntries %v from leader %v", rf.me, args, args.LeaderId)
	// rules for all servers 2
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	// rules for candidates 3
	if rf.state >= Candidate && args.Term >= rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	// rules for followers 2
	if rf.state == Follower && args.Term >= rf.currentTerm {
		dropAndSet(rf.appendEntriesReceivedCh)
	}

	// default reply
	reply.Success = true
	reply.Term = rf.currentTerm

	// AppendEntries RPC receiver implementation 1
	if args.Term < rf.currentTerm {
		DPrintf("[raft] [AppendEntries] [server %v]: reply false because its term %v > AppendEntries' term %v", rf.me, rf.currentTerm, args.Term)
		reply.Success = false
		return
	}

	// AppendEntries RPC receiver implementation 2
	if args.PrevLogIndex <= rf.getLastLogIndex() {
		if args.PrevLogIndex < rf.lastIncludedIndex {
			reply.Success = false
			reply.ConflictIndex = rf.lastIncludedIndex + 1
			DPrintf("[raft] [AppendEntries] [server %v]: reply false because AppendEntries' PrevLogIndex %v < lastIncludedIndex %v", rf.me, args.PrevLogIndex, rf.lastIncludedIndex)
			DPrintf("[raft] [AppendEntries] [server %v]: reply ConflictIndex %v", rf.me, reply.ConflictIndex)
			return
		} else if args.PrevLogIndex == rf.lastIncludedIndex {
			if args.PrevLogTerm != rf.lastIncludedTerm {
				reply.Success = false
				reply.ConflictIndex = rf.lastIncludedIndex
				DPrintf("[raft] [AppendEntries] [server %v]: reply false because AppendEntries' PrevLogTerm %v != lastIncludedTerm %v at index %v", rf.me, args.PrevLogTerm, rf.lastIncludedTerm, args.PrevLogIndex)
				DPrintf("[raft] [AppendEntries] [server %v]: reply ConflictIndex %v", rf.me, reply.ConflictIndex)
				return
			}
		} else {
			prevLog := rf.getLogByIndex(args.PrevLogIndex)
			if prevLog.Term != args.PrevLogTerm {
				reply.Success = false

				// optimization: find first log in the unmatched term
				for _, logEntry := range rf.log {
					if logEntry.Term == prevLog.Term {
						reply.ConflictIndex = logEntry.Index
						break
					}
				}
				DPrintf("[raft] [AppendEntries] [server %v]: reply false because AppendEntries' PrevLogTerm %v != lastIncludedTerm %v at index %v", rf.me, args.PrevLogTerm, rf.lastIncludedTerm, args.PrevLogIndex)
				DPrintf("[raft] [AppendEntries] [server %v]: reply ConflictIndex %v", rf.me, reply.ConflictIndex)
				return
			}
		}
	} else {
		reply.Success = false
		reply.ConflictIndex = rf.getLastStoredLogIndex() + 1
		DPrintf("[raft] [AppendEntries] [server %v]: reply false because AppendEntries' PrevLogIndex %v > highest log index %v", rf.me, args.PrevLogIndex, rf.getLastLogIndex())
		DPrintf("[raft] [AppendEntries] [server %v]: reply ConflictIndex %v", rf.me, reply.ConflictIndex)
		return
	}

	// AppendEntries RPC receiver implementation 3
	if len(args.Entries) > 0 && len(rf.log) > 1 {
		firstStoredLogIndex := rf.getFirstStoredLogIndex()
		lastStoredLogIndex := rf.getLastStoredLogIndex()
		for _, newEntry := range args.Entries {
			if inBetween(newEntry.Index, firstStoredLogIndex, lastStoredLogIndex) {
				if newEntry.Term != rf.getLogByIndex(newEntry.Index).Term {
					rf.log = append([]LogEntry{}, rf.log[:rf.getSliceIndex(newEntry.Index)]...)
					DPrintf("[raft] [AppendEntries] [server %v]: delete conflicted log at index %v, and all that follow it", rf.me, newEntry.Index)
					break
				}
			}
		}
	}

	// AppendEntries RPC receiver implementation 4
	if len(args.Entries) > 0 {
		if len(rf.log) > 1 {
			lastStoredLogIndex := rf.getLastStoredLogIndex()
			for i, newEntry := range args.Entries {
				if newEntry.Index == lastStoredLogIndex+1 {
					rf.log = append(rf.log, args.Entries[i:]...)
					DPrintf("[raft] [AppendEntries] [server %v]: append new entries index start from %v", rf.me, newEntry.Index)
					break
				}
			}
		} else {
			rf.log = append(rf.log, args.Entries...)
			DPrintf("[raft] [AppendEntries] [server %v]: don't have log, append all entries index start from %v", rf.me, rf.log[1].Index)
		}
	}

	// AppendEntries RPC receiver implementation 5
	if args.LeaderCommit > rf.commitIndex {
		DPrintf("[raft] [AppendEntries] [server %v]: commitIndex %v < AppendEntries' LeaderCommit %v", rf.me, rf.commitIndex)
		rf.commitIndex = minInt(args.LeaderCommit, rf.getLastLogIndex())
		DPrintf("[raft] [AppendEntries] [server %v]: set commitIndex to %v", rf.me, rf.commitIndex)
		rf.notifyToApplyCh <- true
		DPrintf("[raft] [AppendEntries] [server %v]: notified applier to apply", rf.me)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()

	DPrintf("[raft] [InstallSnapshot] [server %v]: received AppendEntries %v from leader %v", rf.me, args, args.LeaderId)
	reply.Term = rf.currentTerm

	// InstallSnapshot RPC receiver implementation 1
	if args.Term < rf.currentTerm {
		return
	}

	// InstallSnapshot RPC receiver implementation 5
	if rf.lastIncludedIndex < args.LastIncludedIndex {
		dropAndSet(rf.appendEntriesReceivedCh)

		rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), args.Data)
		DPrintf("[raft] [InstallSnapshot] [server %v]: save snapshot from leader %v", rf.me, args.LeaderId)

		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		DPrintf("[raft] [InstallSnapshot] [server %v]: set lastIncludedIndex to %v, set lastIncludedTerm", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm)

		rf.lastApplied = maxInt(args.LastIncludedIndex, rf.lastApplied)
		DPrintf("[raft] [InstallSnapshot] [server %v]: set lastApplied to %v", rf.me, rf.lastApplied)

		rf.commitIndex = maxInt(rf.commitIndex, args.LastIncludedIndex)
		DPrintf("[raft] [InstallSnapshot] [server %v]: set commitIndex to %v", rf.me, rf.commitIndex)
	}

	// InstallSnapshot RPC receiver implementation 6
	if inBetween(args.LastIncludedIndex, rf.getFirstStoredLogIndex(), rf.getLastStoredLogIndex()) {
		comparedLog := rf.getLogByIndex(args.LastIncludedIndex)

		if comparedLog.Term == args.LastIncludedTerm {
			followingLogIndex := comparedLog.Index + 1

			if followingLogIndex <= rf.getLastStoredLogIndex() {
				followingLogEntries := rf.log[followingLogIndex:]
				rf.log = append([]LogEntry{{0, "EmptyCommand", 0}}, followingLogEntries...)
				DPrintf("[raft] [InstallSnapshot] [server %v]: retain log index start from %v", rf.me, followingLogIndex)
			} else {
				rf.log = []LogEntry{{0, "EmptyCommand", 0}}
				DPrintf("[raft] [InstallSnapshot] [server %v]: don't have log to retain, discard the entire log", rf.me)
			}

			return
		}
	}

	// InstallSnapshot RPC receiver implementation 7
	rf.log = []LogEntry{{0, "EmptyCommand", 0}}
	DPrintf("[raft] [InstallSnapshot] [server %v]: discard the entire log", rf.me)

	// InstallSnapshot RPC receiver implementation 8
	rf.applyCh <- ApplyMsg{false, "ReadSnapshot", rf.lastIncludedIndex, rf.lastIncludedTerm}
	DPrintf("[raft] [InstallSnapshot] [server %v]: notified applier to read snapshot from leader %v, lastIncludedIndex %v", rf.me, args.LeaderId, rf.lastIncludedIndex)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()

	DPrintf("[raft] [RequestVote] [server %v]: received RequestVote from server %v", rf.me, args.CandidateId)
	// rules for all servers 2
	if args.Term > rf.currentTerm {
		DPrintf("[raft] [RequestVote] [server %v]: convert to follower because RequestVote's term %v > server's term %v, ", rf.me, args.Term, rf.currentTerm)
		rf.convertToFollower(args.Term)
	}

	// default reply
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// RequestVote RPC receiver implementation 1
	if args.Term < rf.currentTerm {
		DPrintf("[raft] [RequestVote] [server %v]: reply false because RequestVote's term %v < server's term %v, ", rf.me, args.Term, rf.currentTerm)
		reply.VoteGranted = false
		return
	}

	// RequestVote RPC receiver implementation 2
	candidateLogUpToDate := false
	if args.LastLogTerm > rf.getLastLogTerm() {
		candidateLogUpToDate = true
	} else if args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex() {
		candidateLogUpToDate = true
	}
	if candidateLogUpToDate && (rf.votedFor == NotVoted || rf.votedFor == args.CandidateId) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		DPrintf("[raft] [RequestVote] [server %v]: vote for %v", rf.me, rf.votedFor)
		dropAndSet(rf.voteGrantedCh)
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
