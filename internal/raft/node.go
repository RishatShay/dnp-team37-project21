package raft

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"sort"
	"strings"
	"time"

	"project21/internal/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Role string

const (
	RoleFollower  Role = "follower"
	RoleCandidate Role = "candidate"
	RoleLeader    Role = "leader"
)

type Config struct {
	ID                string
	Address           string
	Peers             map[string]string
	ElectionMin       time.Duration
	ElectionJitter    time.Duration
	HeartbeatInterval time.Duration
	RPCTimeout        time.Duration
}

type Node struct {
	pb.UnimplementedRaftServiceServer
	id      string
	address string
	peers   map[string]string

	store   *LogStore
	sm      *StateMachine
	metrics *MetricsCollector

	role        Role
	currentTerm int64
	votedFor    string
	leaderID    string

	commitIndex int64
	lastApplied int64
	nextIndex   map[string]int64
	matchIndex  map[string]int64

	electionMin       time.Duration
	electionJitter    time.Duration
	heartbeatInterval time.Duration
	rpcTimeout        time.Duration
	rng               *rand.Rand

	requestVoteCh   chan requestVoteRPC
	appendEntriesCh chan appendEntriesRPC
	submitEntryCh   chan submitEntryRPC
	queryLogCh      chan queryLogRPC
	metricsCh       chan metricsRPC
}

type requestVoteRPC struct {
	req  *pb.RequestVoteRequest
	resp chan *pb.RequestVoteResponse
}

type appendEntriesRPC struct {
	req  *pb.AppendEntriesRequest
	resp chan *pb.AppendEntriesResponse
}

type submitEntryRPC struct {
	req  *pb.SubmitEntryRequest
	resp chan *pb.SubmitEntryResponse
}

type queryLogRPC struct {
	req  *pb.QueryLogRequest
	resp chan *pb.QueryLogResponse
}

type metricsRPC struct {
	req  *pb.GetMetricsRequest
	resp chan *pb.GetMetricsResponse
}

var _ pb.RaftServiceServer = (*Node)(nil)

func NewNode(cfg Config, store *LogStore, sm *StateMachine, metrics *MetricsCollector) (*Node, error) {
	if strings.TrimSpace(cfg.ID) == "" {
		return nil, errors.New("node id is required")
	}
	if cfg.ElectionMin == 0 {
		cfg.ElectionMin = 700 * time.Millisecond
	}
	if cfg.ElectionJitter == 0 {
		cfg.ElectionJitter = 700 * time.Millisecond
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 150 * time.Millisecond
	}
	if cfg.RPCTimeout == 0 {
		cfg.RPCTimeout = 450 * time.Millisecond
	}

	peers := make(map[string]string)
	for id, address := range cfg.Peers {
		id = strings.TrimSpace(id)
		address = strings.TrimSpace(address)
		if id == "" || address == "" || id == cfg.ID {
			continue
		}
		peers[id] = address
	}

	term, err := store.CurrentTerm()
	if err != nil {
		return nil, err
	}
	votedFor, err := store.VotedFor()
	if err != nil {
		return nil, err
	}
	commitIndex, err := store.CommitIndex()
	if err != nil {
		return nil, err
	}
	lastIndex, _, err := store.LastIndexAndTerm()
	if err != nil {
		return nil, err
	}
	if commitIndex > lastIndex {
		commitIndex = lastIndex
		if err := store.SetCommitIndex(commitIndex); err != nil {
			return nil, err
		}
	}

	n := &Node{
		id:                cfg.ID,
		address:           cfg.Address,
		peers:             peers,
		store:             store,
		sm:                sm,
		metrics:           metrics,
		role:              RoleFollower,
		currentTerm:       term,
		votedFor:          votedFor,
		commitIndex:       commitIndex,
		nextIndex:         map[string]int64{cfg.ID: lastIndex + 1},
		matchIndex:        map[string]int64{cfg.ID: lastIndex},
		electionMin:       cfg.ElectionMin,
		electionJitter:    cfg.ElectionJitter,
		heartbeatInterval: cfg.HeartbeatInterval,
		rpcTimeout:        cfg.RPCTimeout,
		rng:               rand.New(rand.NewSource(time.Now().UnixNano() + hashID(cfg.ID))),
		requestVoteCh:     make(chan requestVoteRPC),
		appendEntriesCh:   make(chan appendEntriesRPC),
		submitEntryCh:     make(chan submitEntryRPC),
		queryLogCh:        make(chan queryLogRPC),
		metricsCh:         make(chan metricsRPC),
	}
	if err := n.applyCommitted(); err != nil {
		return nil, err
	}
	return n, nil
}

func (n *Node) Start(ctx context.Context) {
	go n.loop(ctx)
}

func (n *Node) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	reply := make(chan *pb.RequestVoteResponse, 1)
	select {
	case n.requestVoteCh <- requestVoteRPC{req: req, resp: reply}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case resp := <-reply:
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (n *Node) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	reply := make(chan *pb.AppendEntriesResponse, 1)
	select {
	case n.appendEntriesCh <- appendEntriesRPC{req: req, resp: reply}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case resp := <-reply:
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (n *Node) SubmitEntry(ctx context.Context, req *pb.SubmitEntryRequest) (*pb.SubmitEntryResponse, error) {
	reply := make(chan *pb.SubmitEntryResponse, 1)
	select {
	case n.submitEntryCh <- submitEntryRPC{req: req, resp: reply}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case resp := <-reply:
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (n *Node) QueryLog(ctx context.Context, req *pb.QueryLogRequest) (*pb.QueryLogResponse, error) {
	reply := make(chan *pb.QueryLogResponse, 1)
	select {
	case n.queryLogCh <- queryLogRPC{req: req, resp: reply}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case resp := <-reply:
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (n *Node) GetMetrics(ctx context.Context, req *pb.GetMetricsRequest) (*pb.GetMetricsResponse, error) {
	reply := make(chan *pb.GetMetricsResponse, 1)
	select {
	case n.metricsCh <- metricsRPC{req: req, resp: reply}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case resp := <-reply:
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (n *Node) loop(ctx context.Context) {
	electionTimer := time.NewTimer(n.nextElectionTimeout())
	heartbeatTicker := time.NewTicker(n.heartbeatInterval)
	defer electionTimer.Stop()
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case rpc := <-n.requestVoteCh:
			resp, resetElection := n.handleRequestVote(rpc.req)
			rpc.resp <- resp
			if resetElection {
				resetTimer(electionTimer, n.nextElectionTimeout())
			}
		case rpc := <-n.appendEntriesCh:
			resp, resetElection := n.handleAppendEntries(rpc.req)
			rpc.resp <- resp
			if resetElection {
				resetTimer(electionTimer, n.nextElectionTimeout())
			}
		case rpc := <-n.submitEntryCh:
			rpc.resp <- n.handleSubmitEntry(rpc.req)
		case rpc := <-n.queryLogCh:
			rpc.resp <- n.handleQueryLog(rpc.req)
		case rpc := <-n.metricsCh:
			rpc.resp <- n.handleGetMetrics(rpc.req)
		case <-electionTimer.C:
			if n.role != RoleLeader {
				n.startElection()
			}
			resetTimer(electionTimer, n.nextElectionTimeout())
		case <-heartbeatTicker.C:
			if n.role == RoleLeader {
				n.broadcastReplication()
			}
		}
	}
}

func (n *Node) handleRequestVote(req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, bool) {
	if req.Term < n.currentTerm {
		return &pb.RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}, false
	}
	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term)
	}

	grant := false
	if (n.votedFor == "" || n.votedFor == req.CandidateId) && n.candidateLogIsUpToDate(req.LastLogIndex, req.LastLogTerm) {
		n.votedFor = req.CandidateId
		if err := n.store.SetVotedFor(n.votedFor); err != nil {
			log.Printf("node=%s persist vote failed: %v", n.id, err)
			return &pb.RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}, false
		}
		grant = true
		log.Printf("node=%s voted_for=%s term=%d", n.id, req.CandidateId, n.currentTerm)
	}

	return &pb.RequestVoteResponse{Term: n.currentTerm, VoteGranted: grant}, grant
}

func (n *Node) handleAppendEntries(req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, bool) {
	if req.Term < n.currentTerm {
		return &pb.AppendEntriesResponse{Term: n.currentTerm, Success: false, MatchIndex: n.lastIndex()}, false
	}
	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term)
	} else if n.role != RoleFollower {
		n.role = RoleFollower
	}

	n.leaderID = req.LeaderId
	if req.PrevLogIndex > 0 {
		prevTerm, err := n.store.TermAt(req.PrevLogIndex)
		if err != nil || prevTerm != req.PrevLogTerm {
			return &pb.AppendEntriesResponse{Term: n.currentTerm, Success: false, MatchIndex: n.lastIndex()}, true
		}
	}

	if err := n.appendLeaderEntries(req.Entries); err != nil {
		log.Printf("node=%s append entries failed: %v", n.id, err)
		return &pb.AppendEntriesResponse{Term: n.currentTerm, Success: false, MatchIndex: n.lastIndex()}, true
	}

	lastIndex := n.lastIndex()
	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = min(req.LeaderCommit, lastIndex)
		if err := n.store.SetCommitIndex(n.commitIndex); err != nil {
			log.Printf("node=%s persist commit index failed: %v", n.id, err)
		}
		if err := n.applyCommitted(); err != nil {
			log.Printf("node=%s apply committed failed: %v", n.id, err)
		}
	}

	matchIndex := req.PrevLogIndex
	if len(req.Entries) > 0 {
		matchIndex = req.Entries[len(req.Entries)-1].Index
	}
	return &pb.AppendEntriesResponse{Term: n.currentTerm, Success: true, MatchIndex: matchIndex}, true
}

func (n *Node) handleSubmitEntry(req *pb.SubmitEntryRequest) *pb.SubmitEntryResponse {
	command := strings.TrimSpace(req.Command)
	if command == "" {
		return &pb.SubmitEntryResponse{Ok: false, LeaderId: n.leaderID, Message: "empty command"}
	}
	if n.role != RoleLeader {
		return &pb.SubmitEntryResponse{
			Ok:       false,
			LeaderId: n.leaderID,
			Message:  fmt.Sprintf("node %s is %s", n.id, n.role),
		}
	}

	entry, err := n.store.Append(n.currentTerm, command)
	if err != nil {
		log.Printf("node=%s append client entry failed: %v", n.id, err)
		return &pb.SubmitEntryResponse{Ok: false, LeaderId: n.id, Message: err.Error()}
	}
	n.metrics.RecordSubmit(entry.Index)
	n.matchIndex[n.id] = entry.Index
	n.nextIndex[n.id] = entry.Index + 1

	n.broadcastReplication()
	if n.role != RoleLeader {
		return &pb.SubmitEntryResponse{Ok: false, LeaderId: n.leaderID, Message: "leadership changed during replication"}
	}
	if n.commitIndex >= entry.Index {
		return &pb.SubmitEntryResponse{
			Ok:       true,
			LeaderId: n.id,
			Index:    entry.Index,
			Term:     entry.Term,
			Message:  "committed by majority",
		}
	}

	return &pb.SubmitEntryResponse{
		Ok:       false,
		LeaderId: n.id,
		Index:    entry.Index,
		Term:     entry.Term,
		Message:  "entry stored on leader but majority was not reached",
	}
}

func (n *Node) handleQueryLog(_ *pb.QueryLogRequest) *pb.QueryLogResponse {
	entries, err := n.store.AllEntries()
	if err != nil {
		log.Printf("node=%s query log failed: %v", n.id, err)
	}

	return &pb.QueryLogResponse{
		NodeId:      n.id,
		Role:        string(n.role),
		LeaderId:    n.leaderID,
		CurrentTerm: n.currentTerm,
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
		Entries:     entries,
		State:       n.sm.Snapshot(),
	}
}

func (n *Node) handleGetMetrics(_ *pb.GetMetricsRequest) *pb.GetMetricsResponse {
	lag := n.commitIndex - n.lastApplied
	if lag < 0 {
		lag = 0
	}
	return &pb.GetMetricsResponse{
		NodeId:         n.id,
		Role:           string(n.role),
		CurrentTerm:    n.currentTerm,
		CommitIndex:    n.commitIndex,
		LastApplied:    n.lastApplied,
		ReplicationLag: lag,
		Metrics:        n.metrics.Snapshot(),
	}
}

func (n *Node) startElection() {
	n.role = RoleCandidate
	n.currentTerm++
	n.votedFor = n.id
	n.leaderID = ""
	if err := n.store.SetCurrentTerm(n.currentTerm); err != nil {
		log.Printf("node=%s persist term failed: %v", n.id, err)
		return
	}
	if err := n.store.SetVotedFor(n.votedFor); err != nil {
		log.Printf("node=%s persist self vote failed: %v", n.id, err)
		return
	}

	term := n.currentTerm
	lastIndex, lastTerm, err := n.store.LastIndexAndTerm()
	if err != nil {
		log.Printf("node=%s read last log for election failed: %v", n.id, err)
		return
	}

	votes := 1
	log.Printf("node=%s starts election term=%d", n.id, term)
	if votes >= n.majority() {
		n.becomeLeader()
		n.broadcastReplication()
		return
	}
	for _, peerID := range n.sortedPeers() {
		resp, err := n.callRequestVote(peerID, &pb.RequestVoteRequest{
			Term:         term,
			CandidateId:  n.id,
			LastLogIndex: lastIndex,
			LastLogTerm:  lastTerm,
		})
		if err != nil {
			continue
		}
		if resp.Term > n.currentTerm {
			n.becomeFollower(resp.Term)
			return
		}
		if n.role != RoleCandidate || n.currentTerm != term {
			return
		}
		if resp.VoteGranted {
			votes++
		}
		if votes >= n.majority() {
			n.becomeLeader()
			n.broadcastReplication()
			return
		}
	}
}

func (n *Node) becomeFollower(term int64) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
		if err := n.store.SetCurrentTerm(term); err != nil {
			log.Printf("node=%s persist follower term failed: %v", n.id, err)
		}
		if err := n.store.SetVotedFor(""); err != nil {
			log.Printf("node=%s clear vote failed: %v", n.id, err)
		}
	}
	if n.role != RoleFollower {
		log.Printf("node=%s becomes follower term=%d", n.id, n.currentTerm)
	}
	n.role = RoleFollower
}

func (n *Node) becomeLeader() {
	lastIndex := n.lastIndex()
	n.role = RoleLeader
	n.leaderID = n.id
	n.nextIndex = make(map[string]int64, len(n.peers)+1)
	n.matchIndex = make(map[string]int64, len(n.peers)+1)
	n.nextIndex[n.id] = lastIndex + 1
	n.matchIndex[n.id] = lastIndex
	for peerID := range n.peers {
		n.nextIndex[peerID] = lastIndex + 1
		n.matchIndex[peerID] = 0
	}
	log.Printf("node=%s becomes leader term=%d", n.id, n.currentTerm)
}

func (n *Node) broadcastReplication() {
	if n.role != RoleLeader {
		return
	}
	for _, peerID := range n.sortedPeers() {
		n.replicatePeer(peerID)
		if n.role != RoleLeader {
			return
		}
	}
	if n.advanceCommitIndex() {
		for _, peerID := range n.sortedPeers() {
			n.replicatePeer(peerID)
			if n.role != RoleLeader {
				return
			}
		}
	}
}

func (n *Node) replicatePeer(peerID string) bool {
	for attempts := 0; attempts < 64 && n.role == RoleLeader; attempts++ {
		next := n.nextIndex[peerID]
		if next < 1 {
			next = 1
		}

		prevIndex := next - 1
		prevTerm, err := n.store.TermAt(prevIndex)
		if err != nil {
			n.nextIndex[peerID] = max(1, next-1)
			continue
		}
		entries, err := n.store.EntriesFrom(next)
		if err != nil {
			log.Printf("node=%s load entries for %s failed: %v", n.id, peerID, err)
			return false
		}

		resp, err := n.callAppendEntries(peerID, &pb.AppendEntriesRequest{
			Term:         n.currentTerm,
			LeaderId:     n.id,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: n.commitIndex,
		})
		if err != nil {
			return false
		}
		if resp.Term > n.currentTerm {
			n.becomeFollower(resp.Term)
			return false
		}
		if resp.Success {
			match := resp.MatchIndex
			if match < prevIndex {
				match = prevIndex
			}
			n.matchIndex[peerID] = match
			n.nextIndex[peerID] = match + 1
			return true
		}
		n.nextIndex[peerID] = max(1, next-1)
	}
	return false
}

func (n *Node) advanceCommitIndex() bool {
	lastIndex := n.lastIndex()
	for index := lastIndex; index > n.commitIndex; index-- {
		term, err := n.store.TermAt(index)
		if err != nil || term != n.currentTerm {
			continue
		}

		replicas := 0
		if n.matchIndex[n.id] >= index {
			replicas++
		}
		for peerID := range n.peers {
			if n.matchIndex[peerID] >= index {
				replicas++
			}
		}

		if replicas >= n.majority() {
			oldCommit := n.commitIndex
			n.commitIndex = index
			if err := n.store.SetCommitIndex(n.commitIndex); err != nil {
				log.Printf("node=%s persist commit index failed: %v", n.id, err)
			}
			for committed := oldCommit + 1; committed <= n.commitIndex; committed++ {
				n.metrics.RecordCommit(committed)
			}
			if err := n.applyCommitted(); err != nil {
				log.Printf("node=%s apply committed failed: %v", n.id, err)
			}
			return true
		}
	}
	return false
}

func (n *Node) appendLeaderEntries(entries []*pb.LogEntry) error {
	for i, incoming := range entries {
		existing, err := n.store.Get(incoming.Index)
		if err == nil {
			if existing.Term == incoming.Term && existing.Command == incoming.Command {
				continue
			}
			if err := n.store.TruncateFrom(incoming.Index); err != nil {
				return err
			}
			return n.appendRemaining(entries[i:])
		}
		if errors.Is(err, ErrLogNotFound) {
			return n.appendRemaining(entries[i:])
		}
		return err
	}
	return nil
}

func (n *Node) appendRemaining(entries []*pb.LogEntry) error {
	for _, entry := range entries {
		if err := n.store.AppendEntry(entry); err != nil {
			return err
		}
	}
	return nil
}

func (n *Node) applyCommitted() error {
	for n.lastApplied < n.commitIndex {
		entry, err := n.store.Get(n.lastApplied + 1)
		if err != nil {
			return err
		}
		n.sm.Apply(entry)
		n.metrics.RecordApply(entry.Index)
		n.lastApplied = entry.Index
	}
	return nil
}

func (n *Node) candidateLogIsUpToDate(candidateLastIndex, candidateLastTerm int64) bool {
	lastIndex, lastTerm, err := n.store.LastIndexAndTerm()
	if err != nil {
		log.Printf("node=%s read last log failed: %v", n.id, err)
		return false
	}
	if candidateLastTerm != lastTerm {
		return candidateLastTerm > lastTerm
	}
	return candidateLastIndex >= lastIndex
}

func (n *Node) callRequestVote(peerID string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	conn, err := n.dialPeer(peerID)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout)
	defer cancel()
	return pb.NewRaftServiceClient(conn).RequestVote(ctx, req)
}

func (n *Node) callAppendEntries(peerID string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	conn, err := n.dialPeer(peerID)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout)
	defer cancel()
	return pb.NewRaftServiceClient(conn).AppendEntries(ctx, req)
}

func (n *Node) dialPeer(peerID string) (*grpc.ClientConn, error) {
	address, ok := n.peers[peerID]
	if !ok {
		return nil, fmt.Errorf("unknown peer %s", peerID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout)
	defer cancel()
	return grpc.DialContext(ctx, address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
}

func (n *Node) sortedPeers() []string {
	ids := make([]string, 0, len(n.peers))
	for id := range n.peers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (n *Node) lastIndex() int64 {
	index, _, err := n.store.LastIndexAndTerm()
	if err != nil {
		log.Printf("node=%s read last index failed: %v", n.id, err)
		return 0
	}
	return index
}

func (n *Node) majority() int {
	return (len(n.peers)+1)/2 + 1
}

func (n *Node) nextElectionTimeout() time.Duration {
	jitter := time.Duration(n.rng.Int63n(int64(n.electionJitter)))
	return n.electionMin + jitter
}

func resetTimer(timer *time.Timer, d time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(d)
}

func hashID(id string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	return int64(h.Sum64())
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
