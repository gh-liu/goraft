package goraft

import (
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

type ServerState uint

const (
	follower  ServerState = iota // follower
	candidata                    // candidata
	leader                       // leader
)

type Server struct {
	address string
	server  *http.Server

	state ServerState

	currentTerm uint64

	clusterIndex uint64
	cluster      []*clusterMember

	heartbeatMs     int
	electionTimeout time.Time

	heartbeatTimeout time.Time

	logs        []Entry
	commitIndex uint64 // index of highest log entry known to be committed
}

func NewServer(address []string, index int) *Server {
	server := &Server{
		clusterIndex: uint64(index),
		address:      address[index],
		heartbeatMs:  300,
	}
	for idx, addr := range address {
		server.cluster = append(server.cluster, &clusterMember{
			address:   addr,
			votedID:   0,
			rpcClient: &rpc.Client{},
			ID:        uint64(idx),
		})

	}
	return server
}

func (s *Server) Start() {
	l, err := net.Listen("tcp", s.address)
	if err != nil {
		panic(err)
	}
	rcpServer := rpc.NewServer()
	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, rcpServer)
	server := http.Server{Handler: mux}
	go server.Serve(l)

	go func() {
		s.resetElectionTimeout()
		for {
			switch s.state {
			case follower:
				if s.isElectionTimeout() {
					s.startNewElection()
				}
				s.applyCommand()
			case candidata:
				if s.isElectionTimeout() {
					s.startNewElection()
				}
				s.becomeLeader()
			case leader:
				if time.Now().After(s.heartbeatTimeout) {
					s.heartbeatTimeout = time.Now().Add(time.Duration(s.heartbeatMs) * time.Millisecond)
					s.appendEntries()
				}
				s.advanceCommitIndex()
				s.applyCommand()
			}
		}
	}()
}

func (s *Server) isElectionTimeout() bool {
	return time.Now().After(s.electionTimeout)
}

func (s *Server) startNewElection() {
	s.state = candidata
	s.currentTerm++
	s.requestVote()
}

func (s *Server) setVotedID(id uint64) {
	s.cluster[s.clusterIndex].setVotedID(id)
}
func (s *Server) getVoteID() uint64 {
	return s.cluster[s.clusterIndex].votedID
}

func (s *Server) ID() uint64 {
	return s.cluster[s.clusterIndex].ID
}

func (s *Server) requestVote() {
	for idx := range s.cluster {
		if idx == int(s.clusterIndex) {
			s.setVotedID(s.ID()) // voted for itself
			continue
		}
		s.cluster[idx].setVotedID(0) // clear

		var req RequestVoteRequest
		req.Term = s.currentTerm
		req.CandidateID = s.ID()
		logLen := len(s.logs) - 1
		lastLogIndex := uint64(logLen)
		lastLogTerm := s.logs[logLen-1].Term
		req.LastLogIndex = lastLogIndex
		req.LastLogTerm = lastLogTerm
		resp, err := s.cluster[idx].requestVote(req)
		if err != nil {
			// TODO: retry
			continue
		}
		if s.currentTerm < resp.Term {
			// Transitioned to follower
			s.currentTerm = resp.Term
			s.state = follower
			s.setVotedID(0)
			s.resetElectionTimeout()
			return
		}
		if s.currentTerm != resp.Term {
			// s.currentTerm > resp.Term
			// NOTE: drop old
			return
		}
		if resp.VoteGranted {
			s.cluster[idx].setVotedID(s.ID())
		}
	}
}

func (s *Server) becomeLeader() {
	quorum := len(s.cluster)/2 + 1
	for i := range s.cluster {
		if s.cluster[i].votedID == s.ID() && quorum > 0 {
			quorum--
		}
	}
	if quorum == 0 {
		// New leader
		s.state = leader
		s.heartbeatTimeout = time.Now()
	}
}

func (s *Server) resetElectionTimeout() {
	interval := time.Duration(rand.Intn(s.heartbeatMs*2) + s.heartbeatMs*2)
	s.electionTimeout = time.Now().Add(interval * time.Millisecond)
}

func (s *Server) HandleRequestVoteRequest(req RequestVoteRequest, resp *RequestVoteResponse) error {
	if s.currentTerm < req.Term {
		// Transitioned to follower
		s.currentTerm = req.Term
		s.state = follower
		s.setVotedID(0)
		s.resetElectionTimeout()
	}

	// Received vote request from ..
	resp.VoteGranted = false
	resp.Term = s.currentTerm

	if s.currentTerm != req.Term {
		// s.currentTerm > resp.Term
		return nil
	}

	// https://github.com/ongardie/raft.tla/blob/6ecbdbcf1bcde2910367cdfd67f31b0bae447ddd/raft.tla#L284
	var lastLogTerm uint64
	logLen := len(s.logs)
	if logLen > 0 {
		// req.LastLogTerm >
		lastLogTerm = s.logs[logLen-1].Term
	}
	logOk := req.LastLogTerm > lastLogTerm || (req.LastLogTerm == lastLogTerm && req.LastLogIndex >= uint64(logLen))
	// NOTE: actually the term is equal
	grant := req.Term == s.currentTerm && logOk && (s.getVoteID() == 0 || s.getVoteID() == req.CandidateID)
	if grant {
		s.setVotedID(req.CandidateID)
		resp.VoteGranted = true
		s.resetElectionTimeout()
	}
	return nil
}

func (s *Server) appendEntries() {
	for idx := range s.cluster {
		if idx == int(s.clusterIndex) {
			continue
		}

		var req AppendEntriesRequest
		req.Term = s.currentTerm
		req.LeaderID = s.ID()
		next := s.cluster[idx].nextIndex
		prevLogIndex := next - 1
		req.PrevLogIndex = prevLogIndex
		req.PrevLogTerm = s.logs[prevLogIndex].Term
		var entries []Entry
		if len(s.logs)-1 > int(s.cluster[idx].nextIndex) {
			entries = s.logs[next:]
		}
		req.Entries = entries
		req.LeaderCommit = s.commitIndex

		resp, err := s.cluster[idx].requestAppendEntries(req)
		if err != nil {
			// TODO: retry
			return
		}

		if s.currentTerm < resp.Term {
			// Transitioned to follower
			s.currentTerm = resp.Term
			s.state = follower
			s.setVotedID(0)
			s.resetElectionTimeout()
			return
		}

		if s.currentTerm != resp.Term && s.state == leader { // why check state here
			// drop
			return
		}

		if resp.Success {
			lenEntries := uint64(len(entries))
			s.cluster[idx].nextIndex = max(req.PrevLogIndex+lenEntries+1, 1)
			s.cluster[idx].matchIndex = prevLogIndex
		} else {
			s.cluster[idx].nextIndex = max(prevLogIndex-1, 1)
		}
	}
}

func (s *Server) advanceCommitIndex() {
	if s.state == leader {
		lastLogIdx := uint64(len(s.logs) - 1)
		for i := lastLogIdx; i > s.commitIndex; i-- {
			quorum := len(s.cluster)/2 + 1

			// check if cluster member agree this commitIndex
			for mbrIdx := range s.cluster {
				if quorum == 0 {
					break
				}
				if (mbrIdx == int(s.clusterIndex)) || (s.cluster[mbrIdx].matchIndex >= i) {
					quorum--
				}
			}

			if quorum == 0 {
				s.commitIndex = i
				break
			}
		}
	}

	// TODO: apply command in the entry
}

func (s *Server) applyCommand() {
	panic("unimplemented")
}

func (s *Server) HandleAppendEntriesRequest(req AppendEntriesRequest, resp *AppendEntriesResponse) error {
	if s.currentTerm < req.Term {
		// Transitioned to follower
		s.currentTerm = req.Term
		s.state = follower
		s.setVotedID(0)
		s.resetElectionTimeout()
	}
	if s.currentTerm > req.Term {
		// from older leader
		return nil
	}
	if s.currentTerm == req.Term && s.state == candidata {
		s.state = follower
	}

	resp.Term = s.currentTerm
	resp.Success = false

	if s.state != follower {
		return nil
	}
	s.resetElectionTimeout()

	logLen := len(s.logs)
	// https://github.com/ongardie/raft.tla/blob/6ecbdbcf1bcde2910367cdfd67f31b0bae447ddd/raft.tla#L328-L331
	prevLogValid := req.PrevLogIndex == 0 ||
		(req.PrevLogIndex <= uint64(logLen) && s.logs[req.PrevLogIndex].Term == req.PrevLogTerm)
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if !prevLogValid {
		return nil
	}

	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	lenLogs := uint64(len(s.logs))
	entryIdx := req.PrevLogIndex + 1
	lenReqEntries := uint64(len(req.Entries))
	for idx := entryIdx; idx < entryIdx+lenReqEntries; idx++ {
		entry := req.Entries[idx-entryIdx]
		if idx < lenLogs && s.logs[idx].Term != entry.Term {
			// same index but different terms
			// delete the existing entry and all that follow it
			s.logs = s.logs[:idx]
		}
		// 4. Append any new entries not already in the log
		if idx >= lenLogs {
			s.logs = append(s.logs, entry)
		}
	}

	if req.LeaderCommit > s.commitIndex {
		// NOTE: why min?
		s.commitIndex = min(req.LeaderCommit, uint64(len(s.logs)-1) /* index of last new entry */)
	}

	return nil
}

type clusterMember struct {
	ID uint64

	address   string
	votedID   uint64
	rpcClient *rpc.Client

	nextIndex  uint64 // index of the next log entry to send to that server
	matchIndex uint64 // index of highest log entry known to be replicated on server
}

func (cm *clusterMember) requestVote(rep RequestVoteRequest) (resp RequestVoteResponse, err error) {
	if cm.rpcClient == nil {
		cm.rpcClient, err = rpc.DialHTTP("tcp", cm.address)
	}
	if err != nil {
		return
	}
	// follower side: handle by [Server.HandleRequestVoteRequest]
	err = cm.rpcClient.Call("Server.HandleRequestVoteRequest", rep, &resp)
	return
}

func (cm *clusterMember) setVotedID(id uint64) {
	cm.votedID = id
}

func (cm *clusterMember) requestAppendEntries(rep AppendEntriesRequest) (resp AppendEntriesResponse, err error) {
	if cm.rpcClient == nil {
		cm.rpcClient, err = rpc.DialHTTP("tcp", cm.address)
	}
	if err != nil {
		return
	}
	// follower side: handle by [Server.HandleAppendEntriesRequest]
	err = cm.rpcClient.Call("Server.HandleAppendEntriesRequest", rep, &resp)
	return
}

type RequestVoteRequest struct {
	Term         uint64 // candidate’s term
	CandidateID  uint64 // candidate requesting vote
	LastLogIndex uint64 // index of candidate’s last log entry
	LastLogTerm  uint64 // term of candidate’s last log entry
}

type RequestVoteResponse struct {
	Term        uint64 // currentTerm, for candidate to update itself
	VoteGranted bool   // true means candidate received vote
}

type Entry struct {
	Term    uint64
	Command interface{}
}

type AppendEntriesRequest struct {
	Term         uint64 // leader’s term
	LeaderID     uint64 // so follower can redirect clients
	PrevLogIndex uint64 // index of log entry immediately preceding new ones
	PrevLogTerm  uint64 // term of prevLogIndex entry

	Entries      []Entry // log entries to store (empty for heartbeat;
	LeaderCommit uint64  // leader’s commitIndex
}

type AppendEntriesResponse struct {
	Term    uint64 // currentTerm, for leader to update itself
	Success bool   // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func min[T ~int | ~uint64](a, b T) T {
	if a < b {
		return a
	}
	return b
}

func max[T ~int | ~uint64](a, b T) T {
	if a > b {
		return a
	}
	return b
}
