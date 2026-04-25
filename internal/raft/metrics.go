package raft

import (
	"sort"
	"sync"
	"time"

	"project21/internal/pb"
)

type MetricsCollector struct {
	mu        sync.Mutex
	submitted map[int64]time.Time
	committed map[int64]time.Time
	applied   map[int64]time.Time
}

func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		submitted: make(map[int64]time.Time),
		committed: make(map[int64]time.Time),
		applied:   make(map[int64]time.Time),
	}
}

func (m *MetricsCollector) RecordSubmit(index int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.submitted[index] = time.Now()
}

func (m *MetricsCollector) RecordCommit(index int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.committed[index]; !exists {
		m.committed[index] = time.Now()
	}
}

func (m *MetricsCollector) RecordApply(index int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.applied[index]; !exists {
		m.applied[index] = time.Now()
	}
}

func (m *MetricsCollector) Snapshot() []*pb.EntryMetric {
	m.mu.Lock()
	defer m.mu.Unlock()

	seen := make(map[int64]struct{})
	for index := range m.submitted {
		seen[index] = struct{}{}
	}
	for index := range m.committed {
		seen[index] = struct{}{}
	}
	for index := range m.applied {
		seen[index] = struct{}{}
	}

	indices := make([]int64, 0, len(seen))
	for index := range seen {
		indices = append(indices, index)
	}
	sort.Slice(indices, func(i, j int) bool { return indices[i] < indices[j] })

	metrics := make([]*pb.EntryMetric, 0, len(indices))
	for _, index := range indices {
		submitted := m.submitted[index]
		committed := m.committed[index]
		applied := m.applied[index]

		var applyDelay int64
		if !committed.IsZero() && !applied.IsZero() {
			applyDelay = applied.Sub(committed).Nanoseconds()
		}

		metrics = append(metrics, &pb.EntryMetric{
			Index:               index,
			SubmittedAtUnixNano: unixNano(submitted),
			CommittedAtUnixNano: unixNano(committed),
			AppliedAtUnixNano:   unixNano(applied),
			ApplyDelayNanos:     applyDelay,
		})
	}
	return metrics
}

func unixNano(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}
