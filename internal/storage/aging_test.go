package storage

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// fixedClock is a controlled clock shared by Memory and SQLite so aging and
// due checks can be asserted deterministically.
type fixedClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFixedClock(now time.Time) *fixedClock { return &fixedClock{now: now} }

func (c *fixedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fixedClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func agingConfig(clk Clock, minWait time.Duration) Config {
	return Config{Clock: clk, PriorityAging: true, PriorityAgingMinWait: minWait}
}

// enqueueAt inserts one message with explicit times so both backends see an
// identical message regardless of their own clock reads.
func enqueueAt(t *testing.T, s Storage, queue string, msg Message) {
	t.Helper()
	if err := s.Enqueue(context.Background(), queue, msg); err != nil {
		t.Fatalf("Enqueue %s: %v", msg.ID, err)
	}
}

// TestPriorityAging_DisabledKeepsPriorityOrder verifies the feature gate: with
// aging off, the historical priority DESC, created_at ASC order is unchanged.
func TestPriorityAging_DisabledKeepsPriorityOrder(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clk := newFixedClock(base)
	m := NewMemory(Config{Clock: clk, PriorityAging: false, PriorityAgingMinWait: time.Minute})

	// An old low-priority message and a newer high-priority one.
	enqueueAt(t, m, "q", Message{ID: "low-old", Priority: 0, CreatedAt: base.Add(-time.Hour), ScheduledAt: base.Add(-time.Hour)})
	enqueueAt(t, m, "q", Message{ID: "high-new", Priority: 2, CreatedAt: base, ScheduledAt: base})

	got, err := m.Dequeue(context.Background(), "q", 10)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if want := []string{"high-new", "low-old"}; !equalIDs(got, want) {
		t.Fatalf("disabled aging order = %v, want %v", ids(got), want)
	}
}

// TestPriorityAging_BeforeThresholdKeepsPriority verifies that a low-priority
// message that has not yet waited MinWait does not jump ahead of higher
// priority work.
func TestPriorityAging_BeforeThresholdKeepsPriority(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clk := newFixedClock(base)
	m := NewMemory(agingConfig(clk, 10*time.Minute))

	// Ready 5 minutes: below the 10 minute floor.
	enqueueAt(t, m, "q", Message{ID: "low-recent", Priority: 0, CreatedAt: base.Add(-5 * time.Minute), ScheduledAt: base.Add(-5 * time.Minute)})
	enqueueAt(t, m, "q", Message{ID: "high-now", Priority: 2, CreatedAt: base, ScheduledAt: base})

	got, err := m.Dequeue(context.Background(), "q", 10)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if want := []string{"high-now", "low-recent"}; !equalIDs(got, want) {
		t.Fatalf("before threshold order = %v, want %v", ids(got), want)
	}
}

// TestPriorityAging_PromotesLongWaitingLow verifies the core guarantee: a
// low-priority message that has been ready for MinWait is delivered ahead of a
// higher-priority message that arrived later.
func TestPriorityAging_PromotesLongWaitingLow(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clk := newFixedClock(base)
	m := NewMemory(agingConfig(clk, 10*time.Minute))

	// Ready exactly at the threshold: qualifies.
	enqueueAt(t, m, "q", Message{ID: "low-aged", Priority: 0, CreatedAt: base.Add(-10 * time.Minute), ScheduledAt: base.Add(-10 * time.Minute)})
	enqueueAt(t, m, "q", Message{ID: "high-new", Priority: 2, CreatedAt: base, ScheduledAt: base})

	got, err := m.Dequeue(context.Background(), "q", 10)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if want := []string{"low-aged", "high-new"}; !equalIDs(got, want) {
		t.Fatalf("aged promotion order = %v, want %v", ids(got), want)
	}
}

// TestPriorityAging_FutureScheduledNotPromoted verifies that a message whose
// scheduled time has not arrived never participates in promotion, even if it
// was created long ago.
func TestPriorityAging_FutureScheduledNotPromoted(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clk := newFixedClock(base)
	m := NewMemory(agingConfig(clk, 10*time.Minute))

	// Created long ago but only ready far in the future.
	enqueueAt(t, m, "q", Message{ID: "future", Priority: 2, CreatedAt: base.Add(-time.Hour), ScheduledAt: base.Add(time.Hour)})
	enqueueAt(t, m, "q", Message{ID: "due-low", Priority: 0, CreatedAt: base, ScheduledAt: base})

	got, err := m.Dequeue(context.Background(), "q", 10)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if want := []string{"due-low"}; !equalIDs(got, want) {
		t.Fatalf("dequeue excluded future = %v, want %v", ids(got), want)
	}
}

// TestPriorityAging_AgedTieBreakByReadyThenID verifies that candidates at the
// same boundary are ordered by ready time and then stable message identity,
// ignoring priority.
func TestPriorityAging_AgedTieBreakByReadyThenID(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clk := newFixedClock(base)
	m := NewMemory(agingConfig(clk, time.Minute))

	// Same ready time: identity decides (b before c), regardless of priority.
	enqueueAt(t, m, "q", Message{ID: "c", Priority: 2, CreatedAt: base.Add(-time.Hour), ScheduledAt: base.Add(-5 * time.Minute)})
	enqueueAt(t, m, "q", Message{ID: "b", Priority: 0, CreatedAt: base.Add(-time.Hour), ScheduledAt: base.Add(-5 * time.Minute)})
	// Older ready time wins over a newer, higher-priority aged message.
	enqueueAt(t, m, "q", Message{ID: "a", Priority: 0, CreatedAt: base.Add(-time.Hour), ScheduledAt: base.Add(-9 * time.Minute)})

	got, err := m.Dequeue(context.Background(), "q", 10)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if want := []string{"a", "b", "c"}; !equalIDs(got, want) {
		t.Fatalf("aged tie-break order = %v, want %v", ids(got), want)
	}
}

// TestPriorityAging_MemorySQLiteParity drives Memory and SQLite from the same
// controlled clock and asserts identical dequeue order under aging.
func TestPriorityAging_MemorySQLiteParity(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	msgs := []Message{
		{ID: "low-aged", Priority: 0, CreatedAt: base.Add(-time.Hour), ScheduledAt: base.Add(-30 * time.Minute)},
		{ID: "normal-aged", Priority: 1, CreatedAt: base.Add(-time.Hour), ScheduledAt: base.Add(-20 * time.Minute)},
		{ID: "high-new", Priority: 2, CreatedAt: base, ScheduledAt: base},
		{ID: "low-recent", Priority: 0, CreatedAt: base.Add(-time.Minute), ScheduledAt: base.Add(-time.Minute)},
		{ID: "future-high", Priority: 2, CreatedAt: base, ScheduledAt: base.Add(time.Hour)},
	}

	run := func(t *testing.T, s Storage) []string {
		t.Helper()
		for _, msg := range msgs {
			enqueueAt(t, s, "q", msg)
		}
		got, err := s.Dequeue(context.Background(), "q", 10)
		if err != nil {
			t.Fatalf("Dequeue: %v", err)
		}
		return ids(got)
	}

	mem := NewMemory(agingConfig(newFixedClock(base), 10*time.Minute))
	memOrder := run(t, mem)

	sq := newTestSQLiteWithConfig(t, agingConfig(newFixedClock(base), 10*time.Minute))
	sqOrder := run(t, sq)

	want := []string{"low-aged", "normal-aged", "high-new", "low-recent"}
	if !equalStrings(memOrder, want) {
		t.Fatalf("memory order = %v, want %v", memOrder, want)
	}
	if !equalStrings(sqOrder, memOrder) {
		t.Fatalf("sqlite order = %v, want same as memory %v", sqOrder, memOrder)
	}
}

// TestPriorityAging_BoundaryAchievedByClockAdvance verifies that a message
// becomes promoted purely because the controlled clock crossed the threshold,
// without any further writes.
func TestPriorityAging_BoundaryAchievedByClockAdvance(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clk := newFixedClock(base)
	m := NewMemory(agingConfig(clk, 10*time.Minute))

	enqueueAt(t, m, "q", Message{ID: "low", Priority: 0, CreatedAt: base.Add(-9 * time.Minute), ScheduledAt: base.Add(-9 * time.Minute)})

	// One minute later the low message has waited 10 minutes and is due; but
	// first let a high-priority message arrive in the same window.
	clk.Advance(time.Minute)
	enqueueAt(t, m, "q", Message{ID: "high", Priority: 2, CreatedAt: clk.Now(), ScheduledAt: clk.Now()})

	got, err := m.Dequeue(context.Background(), "q", 10)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if want := []string{"low", "high"}; !equalIDs(got, want) {
		t.Fatalf("clock-advanced promotion order = %v, want %v", ids(got), want)
	}
}

func newTestSQLiteWithConfig(t *testing.T, cfg Config) *SQLite {
	t.Helper()
	s, err := NewSQLite(context.Background(), filepath.Join(t.TempDir(), "aging.db"), cfg)
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestPriorityAging_PostgresParity drives Postgres from the same controlled
// clock as the other backends and asserts the same dequeue order. It is
// skipped when no Postgres server is reachable.
func TestPriorityAging_PostgresParity(t *testing.T) {
	skipIfNoPostgres(t)
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	seed, err := NewPostgres(ctx, testDSN)
	if err != nil {
		t.Fatalf("NewPostgres: %v", err)
	}
	for _, table := range []string{
		"shoebox_messages", "shoebox_stats", "shoebox_schedules", "shoebox_schema_migrations",
	} {
		if _, err := seed.pool.Exec(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			_ = seed.Close()
			t.Fatalf("drop %s: %v", table, err)
		}
	}
	_ = seed.Close()

	pg, err := NewPostgresWithConfig(ctx, testDSN, agingConfig(newFixedClock(base), 10*time.Minute))
	if err != nil {
		t.Fatalf("NewPostgresWithConfig: %v", err)
	}
	t.Cleanup(func() { _ = pg.Close() })

	msgs := []Message{
		{ID: "low-aged", Priority: 0, CreatedAt: base.Add(-time.Hour), ScheduledAt: base.Add(-30 * time.Minute)},
		{ID: "normal-aged", Priority: 1, CreatedAt: base.Add(-time.Hour), ScheduledAt: base.Add(-20 * time.Minute)},
		{ID: "high-new", Priority: 2, CreatedAt: base, ScheduledAt: base},
		{ID: "low-recent", Priority: 0, CreatedAt: base.Add(-time.Minute), ScheduledAt: base.Add(-time.Minute)},
		{ID: "future-high", Priority: 2, CreatedAt: base, ScheduledAt: base.Add(time.Hour)},
	}
	for _, msg := range msgs {
		enqueueAt(t, pg, "q", msg)
	}

	got, err := pg.Dequeue(ctx, "q", 10)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	want := []string{"low-aged", "normal-aged", "high-new", "low-recent"}
	if !equalIDs(got, want) {
		t.Fatalf("postgres order = %v, want %v", ids(got), want)
	}
}

func equalIDs(got []Message, want []string) bool {
	return equalStrings(ids(got), want)
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
