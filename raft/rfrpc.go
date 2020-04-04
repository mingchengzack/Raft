package raft

import "time"

//
// Raft RPC definitions and handlers
//

// RequestVoteArgs defines RequestVote RPC arguments structure
type RequestVoteArgs struct {
	Term         int // Candidate's term
	CandidateID  int // Candidate that is requesting vote
	LastLogIndex int // Index of candidate’s last log entry
	LastLogTerm  int // Term of candidate’s last log entry
}

// RequestVoteReply RequestVote RPC reply structure
type RequestVoteReply struct {
	Term        int  // Current term for peers to update itself
	VoteGranted bool // True means candidate received vote
}

// isMoreUpToDate is a helper function that determines
// if Raft's log is more up to date than candidate's
// Assuming lock is acquired
func (rf *Raft) isMoreUpToDate(candidateIndex, candidateTerm int) bool {
	// Current log is empty, no way it's more up to date
	if len(rf.log) == 0 {
		return false
	}

	// If current log is not empty but candidate's is empty
	if candidateIndex == 0 {
		return true
	}

	// If term is equal compare index
	// otherwise compare term
	lastLogIndex, lastLogTerm := rf.getLastLog()
	if lastLogTerm == candidateTerm {
		return lastLogIndex > candidateIndex
	}

	return lastLogTerm > candidateTerm
}

// RequestVote defines the RPC handler for requesting vote from peers
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Set reply
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// Receives stale term request
	if rf.currentTerm > args.Term {
		return
	}

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if rf.currentTerm < args.Term {
		rf.convertToFollower(args.Term)
	}

	// If votedFor is null or candidateId
	// and candidate’s log is at least as up-to-date as receiver’s log
	// grant vote
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) &&
		!rf.isMoreUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true

		// Reset election timer only if GRANTING the vote
		rf.electionTimer = time.Now()
	}
}

// AppendEntriesArgs defines AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	Term         int        // Leader's term
	LeaderID     int        // Leader's ID
	PrevLogIndex int        // Index of log entry immediately preceding new ones
	PrevLogTerm  int        // Term of prevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit int        // Leader’s commitIndex
}

// AppendEntriesReply defines AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term    int  // Current term for leader to update itself
	Success bool // True if follower contained entry matching prevLogIndex and prevLogTerm

	// Used for efficient roll-back
	Xterm  int // Term in the conflicting entry (if any)
	XIndex int // Index of first entry with that conflicting term (if any)
	XLen   int // Log length
}

// contains is a helper function that checks if Raft's log
// contains an entry at prevLogIndex whose term matches prevLogTerm from leader
// it also set reply's params that help with efficient roll-back if not contains
// Assuming log index is the current position in log (no snapshot)
func (rf *Raft) contains(prevLogIndex, prevLogTerm int, reply *AppendEntriesReply) bool {
	// Try to add the first log entry from leader's view
	if prevLogIndex == 0 {
		return true
	}

	// prevLogIndex points beyond the end of the log
	if len(rf.log) < prevLogIndex {
		return false
	}

	// Conflicting term
	if term := rf.log[prevLogIndex-1].Term; term != prevLogTerm {
		reply.Xterm = term

		// Find index of first entry with that confliting term
		i := prevLogIndex - 1
		for i > 0 && rf.log[i-1].Term == term {
			i--
		}
		reply.XIndex = rf.log[i].Index

		return false
	}

	return true
}

// AppendEntries defines the RPC handler for appending log entry from leader
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Set reply
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.Xterm = 0
	reply.XIndex = 0
	reply.XLen = len(rf.log)

	// Receives stale term request
	if rf.currentTerm > args.Term {
		return
	}

	// If RPC request or response contains term T > currentTerm
	// Or it is a candidate and receive heartbeat from leader
	// set currentTerm = T, convert to follower
	if rf.currentTerm < args.Term ||
		(rf.currentTerm == args.Term && rf.state == Candidate) {
		rf.convertToFollower(args.Term)
	}

	// Reset election timer only if receives from CURRENT leader
	// (i.e term in arguments should not be outdated)
	rf.electionTimer = time.Now()

	// Check if log contains an entry at prevLogIndex
	// whose term matches prevLogTerm
	if !rf.contains(args.PrevLogIndex, args.PrevLogTerm, reply) {
		return
	}

	// Found matched log
	// Try to append new log entries
	reply.Success = true
	i := args.PrevLogIndex
	for _, le := range args.Entries {
		// Append any new entries not already in the log
		if i >= len(rf.log) {
			rf.log = append(rf.log, le)
		} else {
			// Delete confliting entries and all that follow it
			if rf.log[i].Term != le.Term {
				rf.log = append(rf.log[:i:i], le)
			}
		}
		i++
	}

	// Set commitIndex for followers to catch up with leader's commit
	if rf.commitIndex < args.LeaderCommit {
		if len(args.Entries) == 0 { // Heartbeat for followers to catch up
			rf.commitIndex = args.LeaderCommit
		} else { // Otherwise take min(leaderCommit, index of last entry)
			lastNewEntry := args.Entries[len(args.Entries)-1].Index
			if args.LeaderCommit < lastNewEntry {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = lastNewEntry
			}
		}
		rf.cond.Broadcast()
	}
}

// For sending and receiving RPC
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//

// sendRequestVote sends a RPC request to ask for vote in leader election
// and handles the response from peer, add note or not
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs) {
	// No longer a candidate just return
	rf.mu.Lock()
	if rf.killed() || rf.state != Candidate {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock() // Don't want to lock while sending RPC request

	// Send Requestvote RPC
	reply := &RequestVoteReply{}
	if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Process the reply only when current term doesn't change
	// between sending RPC and receiving RPC
	// and is still a candidate and not dead
	if rf.killed() || rf.currentTerm != args.Term ||
		rf.state != Candidate {
		return
	}

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if rf.currentTerm < reply.Term {
		rf.convertToFollower(reply.Term)
		return
	}

	// If got vote
	if reply.VoteGranted {
		rf.voteCount++
	}

	// Candidate has majority of votes becomes leader
	if rf.voteCount > len(rf.peers)/2 {
		rf.convertToLeader()

		// Act as a leader for each peer server
		go rf.broadcastAppendEntries()
	}
}

// sendAppendEntries sends a RPC request to append log entry or heartbeat
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {
	// Stop if current server stop being a leader or is dead
	rf.mu.Lock()
	if rf.killed() || rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	// Send AppendEntries RPC
	reply := &AppendEntriesReply{}
	if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Process the reply only when current term doesn't change
	// between sending RPC and receiving RPC
	if rf.killed() || rf.currentTerm != args.Term ||
		rf.state != Leader {
		return
	}

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if rf.currentTerm < reply.Term {
		rf.convertToFollower(reply.Term)
		return
	}

	// Success from peer, found matched log
	if reply.Success {
		// Update nextIndex and matchIndex
		if args.PrevLogIndex+len(args.Entries) <= rf.matchIndex[server] {
			return
		}
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		return
	}

	// Log inconsistent
	// Peer doesn't have prevLogIndex in its log
	if reply.Xterm == 0 {
		rf.nextIndex[server] = reply.XLen
	} else {
		rf.nextIndex[server] = reply.XIndex

		// Search the confliting term in log
		i := len(rf.log) - 1
		for i >= 0 && rf.log[i].Term != reply.Xterm {
			i--
		}

		// Found the term
		if i >= 0 {
			rf.nextIndex[server] = rf.log[i].Index
		}
	}

	return
}
