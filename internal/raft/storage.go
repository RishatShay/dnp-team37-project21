package raft

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"project21/internal/pb"
	"strconv"
	"sync"

	_ "modernc.org/sqlite"
)

var ErrLogNotFound = errors.New("log entry not found")

type LogStore struct {
	db *sql.DB
	mu sync.Mutex
}

func OpenLogStore(path string) (*LogStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	store := &LogStore{db: db}
	if err := store.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *LogStore) Close() error {
	return s.db.Close()
}

func (s *LogStore) init() error {
	statements := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=FULL;",
		"PRAGMA busy_timeout=5000;",
		`CREATE TABLE IF NOT EXISTS meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS log_entries (
			idx INTEGER PRIMARY KEY,
			term INTEGER NOT NULL,
			command TEXT NOT NULL
		);`,
	}
	for _, stmt := range statements {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("initialize sqlite: %w", err)
		}
	}

	if err := s.ensureMeta("current_term", "0"); err != nil {
		return err
	}
	if err := s.ensureMeta("voted_for", ""); err != nil {
		return err
	}
	if err := s.ensureMeta("commit_index", "0"); err != nil {
		return err
	}
	return nil
}

func (s *LogStore) ensureMeta(key, value string) error {
	_, err := s.db.Exec(`INSERT OR IGNORE INTO meta(key, value) VALUES(?, ?)`, key, value)
	if err != nil {
		return fmt.Errorf("ensure meta %s: %w", key, err)
	}
	return nil
}

func (s *LogStore) CurrentTerm() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.metaIntLocked("current_term")
}

func (s *LogStore) SetCurrentTerm(term int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.setMetaLocked("current_term", strconv.FormatInt(term, 10))
}

func (s *LogStore) VotedFor() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.metaStringLocked("voted_for")
}

func (s *LogStore) SetVotedFor(nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.setMetaLocked("voted_for", nodeID)
}

func (s *LogStore) CommitIndex() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.metaIntLocked("commit_index")
}

func (s *LogStore) SetCommitIndex(index int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.setMetaLocked("commit_index", strconv.FormatInt(index, 10))
}

func (s *LogStore) Append(term int64, command string) (*pb.LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	lastIndex, _, err := s.lastIndexAndTermLocked()
	if err != nil {
		return nil, err
	}
	entry := &pb.LogEntry{Index: lastIndex + 1, Term: term, Command: command}
	if err := s.appendEntryLocked(entry); err != nil {
		return nil, err
	}
	return entry, nil
}

func (s *LogStore) AppendEntry(entry *pb.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendEntryLocked(entry)
}

func (s *LogStore) Get(index int64) (*pb.LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getLocked(index)
}

func (s *LogStore) TermAt(index int64) (int64, error) {
	if index == 0 {
		return 0, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	entry, err := s.getLocked(index)
	if err != nil {
		return 0, err
	}
	return entry.Term, nil
}

func (s *LogStore) LastIndexAndTerm() (int64, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastIndexAndTermLocked()
}

func (s *LogStore) EntriesFrom(index int64) ([]*pb.LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rows, err := s.db.Query(`SELECT idx, term, command FROM log_entries WHERE idx >= ? ORDER BY idx`, index)
	if err != nil {
		return nil, fmt.Errorf("query entries from %d: %w", index, err)
	}
	defer rows.Close()

	var entries []*pb.LogEntry
	for rows.Next() {
		entry := &pb.LogEntry{}
		if err := rows.Scan(&entry.Index, &entry.Term, &entry.Command); err != nil {
			return nil, fmt.Errorf("scan log entry: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate log entries: %w", err)
	}
	return entries, nil
}

func (s *LogStore) AllEntries() ([]*pb.LogEntry, error) {
	return s.EntriesFrom(1)
}

func (s *LogStore) TruncateFrom(index int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.db.Exec(`DELETE FROM log_entries WHERE idx >= ?`, index); err != nil {
		return fmt.Errorf("truncate log from %d: %w", index, err)
	}
	return nil
}

func (s *LogStore) metaStringLocked(key string) (string, error) {
	var value string
	err := s.db.QueryRow(`SELECT value FROM meta WHERE key = ?`, key).Scan(&value)
	if err != nil {
		return "", fmt.Errorf("read meta %s: %w", key, err)
	}
	return value, nil
}

func (s *LogStore) metaIntLocked(key string) (int64, error) {
	value, err := s.metaStringLocked(key)
	if err != nil {
		return 0, err
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse meta %s=%q: %w", key, value, err)
	}
	return parsed, nil
}

func (s *LogStore) setMetaLocked(key, value string) error {
	_, err := s.db.Exec(`INSERT INTO meta(key, value) VALUES(?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
	if err != nil {
		return fmt.Errorf("set meta %s: %w", key, err)
	}
	return nil
}

func (s *LogStore) appendEntryLocked(entry *pb.LogEntry) error {
	_, err := s.db.Exec(`INSERT INTO log_entries(idx, term, command) VALUES(?, ?, ?)`, entry.Index, entry.Term, entry.Command)
	if err != nil {
		return fmt.Errorf("append log entry %d: %w", entry.Index, err)
	}
	return nil
}

func (s *LogStore) getLocked(index int64) (*pb.LogEntry, error) {
	entry := &pb.LogEntry{}
	err := s.db.QueryRow(`SELECT idx, term, command FROM log_entries WHERE idx = ?`, index).
		Scan(&entry.Index, &entry.Term, &entry.Command)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrLogNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get log entry %d: %w", index, err)
	}
	return entry, nil
}

func (s *LogStore) lastIndexAndTermLocked() (int64, int64, error) {
	var index, term sql.NullInt64
	err := s.db.QueryRow(`SELECT idx, term FROM log_entries ORDER BY idx DESC LIMIT 1`).Scan(&index, &term)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, fmt.Errorf("last log entry: %w", err)
	}
	return index.Int64, term.Int64, nil
}
