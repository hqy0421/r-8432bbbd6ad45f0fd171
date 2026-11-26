// Package storage defines the Storage interface that abstracts over the
// queue's persistence layer. The broker only ever talks to Storage; the
// concrete backend (Memory, SQLite, Postgres) is selected at New() time.
package storage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strconv"
	"time"
)

// Clock supplies the current time. It exists so tests can drive Memory and
// SQLite from the same controlled clock and assert identical dequeue order.
// A nil Clock means real time.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// Config carries backend construction settings that affect time and dequeue
// ordering. The zero value means real time and the existing priority order
// (priority DESC, created_at ASC).
type Config struct {
	// Clock is the source of "now" for due checks and priority aging. Nil
	// uses real time.
	Clock Clock

	// PriorityAging enables starvation prevention. When false, dequeue
	// order is the existing priority DESC, created_at ASC.
	PriorityAging bool

	// PriorityAgingMinWait is how long a due message must have been ready
	// (ScheduledAt elapsed) before it is promoted ahead of non-aged
	// messages. Promotion orders aged messages by ready time and then
	// stable message identity, ignoring priority. Must be positive when
	// PriorityAging is set.
	PriorityAgingMinWait time.Duration
}

func (c Config) clock() Clock {
	if c.Clock == nil {
		return realClock{}
	}
	return c.Clock
}

// aging reports whether the config enables priority aging.
func (c Config) aging() bool {
	return c.PriorityAging && c.PriorityAgingMinWait > 0
}

func firstConfig(cfgs []Config) Config {
	if len(cfgs) > 0 {
		return cfgs[0]
	}
	return Config{}
}

// aged reports whether msg has been ready for at least minWait at now. A
// message that is not yet due (ScheduledAt after now) is never aged; the
// aging clock starts when the message becomes ready.
func aged(msg Message, now time.Time, minWait time.Duration) bool {
	return !msg.ScheduledAt.After(now.Add(-minWait))
}

// dequeueLess is the canonical dequeue order shared by every backend, so the
// same controlled clock yields the same sequence from Memory and SQLite.
//
// With aging disabled it is the historical order: priority DESC, created_at
// ASC. With aging enabled, due messages that have waited at least the
// threshold come first (ordered by ready time, then message ID), followed by
// the rest in priority order. A message that is not yet due is never aged.
func dequeueLess(a, b Message, now time.Time, cfg Config) bool {
	if cfg.aging() {
		aAged := aged(a, now, cfg.PriorityAgingMinWait)
		bAged := aged(b, now, cfg.PriorityAgingMinWait)
		if aAged != bAged {
			return aAged
		}
		if aAged {
			if !a.ScheduledAt.Equal(b.ScheduledAt) {
				return a.ScheduledAt.Before(b.ScheduledAt)
			}
			return a.ID < b.ID
		}
	}
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	if !a.CreatedAt.Equal(b.CreatedAt) {
		return a.CreatedAt.Before(b.CreatedAt)
	}
	if cfg.aging() {
		return a.ID < b.ID
	}
	return false
}

// Message is the storage-layer view of a queued message. It mirrors the
// public shoebox.Message so callers don't need to translate between the
// two; the broker is the only package that does so.
type Message struct {
	ID          string
	Queue       string
	Payload     []byte
	Attempts    int
	MaxRetries  int
	CreatedAt   time.Time
	ScheduledAt time.Time
	DedupeKey   string
	Metadata    map[string]string

	// Priority is the delivery hint. Higher values are dequeued first
	// (priority DESC) within the same due window; ties break by
	// created_at ASC. Zero (the default) is the lowest priority.
	Priority int

	// Error is set when the message is moved to the dead-letter queue; it
	// holds the last handler error. Empty for live messages.
	Error string

	// DeadAt is set when the message is moved to the dead-letter queue; it
	// records the time of dead-lettering. Zero for live messages.
	DeadAt time.Time
}

// Schedule is a persistent periodic enqueue definition.
type Schedule struct {
	ID             string
	Queue          string
	Payload        []byte
	EnqueueOptions []byte
	Interval       time.Duration

	NextRunAt time.Time
	Enabled   bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

func nonNegativeUint64(v int64) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}

func deadLetterMessage(queue string, msg Message, err error, now time.Time) Message {
	msg.Queue = queue + ".dlq"
	msg.ID = NewMessageID()
	msg.ScheduledAt = now
	msg.DeadAt = now
	msg.Error = ""
	if err != nil {
		msg.Error = err.Error()
	}
	return msg
}

func replayMessage(queue string, msg Message, now time.Time) Message {
	msg.Queue = queue
	msg.ScheduledAt = now
	msg.Error = ""
	msg.DeadAt = time.Time{}
	return msg
}

func periodicMessage(schedule Schedule, now time.Time) Message {
	return Message{
		ID:          NewMessageID(),
		Queue:       schedule.Queue,
		Payload:     append([]byte(nil), schedule.Payload...),
		CreatedAt:   now,
		ScheduledAt: now,
	}
}

// ScheduleStore persists periodic enqueue definitions and executes one
// occurrence atomically with its cadence advance.
type ScheduleStore interface {
	CreateSchedule(ctx context.Context, schedule Schedule) error
	UpdateSchedule(ctx context.Context, schedule Schedule) error
	DeleteSchedule(ctx context.Context, id string) error
	ListSchedules(ctx context.Context, queue string) ([]Schedule, error)
	RunSchedule(ctx context.Context, schedule Schedule, now, next time.Time) (bool, error)
	DueSchedules(ctx context.Context, now time.Time, limit int) ([]Schedule, error)
}

var ErrScheduleExists = errors.New("shoebox/storage: schedule already exists")

// QueueStats is the storage-layer view of queue statistics.
type QueueStats struct {
	Queue     string
	Depth     int
	Processed uint64
	Errors    uint64
	Retries   uint64
	Dead      uint64
}

// ErrEmpty is returned by Dequeue when no messages are available.
var ErrEmpty = errors.New("shoebox/storage: queue empty")

// Storage is the interface every backend implements.
//
// Enqueue persists a new message. EnqueueBatch persists multiple messages in
// one committed operation. Dequeue returns up to `limit` messages that are due
// (ScheduledAt <= now), atomically transitioning them to an in-flight state
// (SQLite/Postgres: status='processing'; Memory: removed from the pending
// slice). Ack and AckBatch confirm successful processing and remove messages.
//
// Retry atomically records a failed delivery and persists the updated
// message for a future delivery. DeadLetter atomically moves a failed
// message to its queue's DLQ and records the terminal outcome. Replay
// atomically moves a DLQ message back to its source queue. List returns
// dead messages from a queue. Reclaim transitions stale in-flight messages
// back to pending (crash recovery; called once during open of a persistent
// backend).
type Storage interface {
	Enqueue(ctx context.Context, queue string, m Message) error
	EnqueueBatch(ctx context.Context, queue string, messages []Message) error
	Dequeue(ctx context.Context, queue string, limit int) ([]Message, error)
	Ack(ctx context.Context, queue string, msgID string) error
	AckBatch(ctx context.Context, queue string, msgIDs []string) error
	Retry(ctx context.Context, queue string, m Message, err error) error
	DeadLetter(ctx context.Context, queue string, m Message, err error) error
	Replay(ctx context.Context, queue string, msgID string) error
	Stats(ctx context.Context, queue string) (QueueStats, error)
	List(ctx context.Context, queue string, limit int) ([]Message, error)
	Reclaim(ctx context.Context, queue string) error
	Close() error
}

// NewMessageID returns a 16-byte random hex string suitable for use as a
// message ID. We avoid pulling in a UUID library to keep the dependency
// surface small (see ADR 0004). If crypto/rand fails (unrecoverable on a
// healthy system) it falls back to a time-based identifier so we never
// panic in a user's hot path.
func NewMessageID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		now := time.Now().UnixNano()
		return "ts-" + hex.EncodeToString([]byte(strconv.FormatInt(now, 10)))
	}
	return hex.EncodeToString(b[:])
}
