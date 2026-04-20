package raft

import (
	"fmt"
	"project21/internal/pb"
	"strings"
	"sync"
)

type StateMachine struct {
	mu sync.Mutex
	kv map[string]string
}

func NewStateMachine() *StateMachine {
	return &StateMachine{kv: make(map[string]string)}
}

func (s *StateMachine) Apply(entry *pb.LogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	command := strings.TrimSpace(entry.Command)
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return
	}

	switch strings.ToUpper(parts[0]) {
	case "SET":
		if len(parts) >= 3 {
			_, afterVerb, _ := strings.Cut(command, parts[0])
			_, value, _ := strings.Cut(strings.TrimSpace(afterVerb), parts[1])
			s.kv[parts[1]] = strings.TrimSpace(value)
			return
		}
	case "DEL", "DELETE":
		if len(parts) >= 2 {
			delete(s.kv, parts[1])
			return
		}
	}

	s.kv[fmt.Sprintf("entry/%d", entry.Index)] = command
}

func (s *StateMachine) Snapshot() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make(map[string]string, len(s.kv))
	for key, value := range s.kv {
		out[key] = value
	}
	return out
}
