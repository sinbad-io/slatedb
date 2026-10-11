package slatedb_test

import (
	"errors"
	"testing"
	"time"

	slatedb "slatedb.io/slatedb-go/uniffi"
)

// Two databases built with one budget count their unflushed writes against
// it, a closed one's bytes leave it, and a budget under a database's
// max_unflushed_bytes is refused at build.
func TestTwoDatabasesCountAgainstOneUnflushedBudget(t *testing.T) {
	store := newMemoryStore(t)
	budget, err := slatedb.NewUnflushedBudget(1 << 30)
	if err != nil {
		t.Fatalf("NewUnflushedBudget(): %v", err)
	}
	defer budget.Destroy()
	if budget.Capacity() != 1<<30 || budget.Used() != 0 {
		t.Fatalf("a new budget: capacity %d used %d, want %d and 0", budget.Capacity(), budget.Used(), 1<<30)
	}
	open := func(path string) *slatedb.Db {
		builder := slatedb.NewDbBuilder(path, store)
		defer builder.Destroy()
		if err := builder.WithUnflushedBudget(budget); err != nil {
			t.Fatalf("WithUnflushedBudget(): %v", err)
		}
		db, err := builder.Build()
		if err != nil {
			t.Fatalf("Build(%s): %v", path, err)
		}
		return db
	}
	dbs := []*slatedb.Db{open("budget-a"), open("budget-b")}
	for _, db := range dbs {
		handle, err := db.Put([]byte("k"), make([]byte, 64<<10))
		if err != nil {
			t.Fatalf("Put(): %v", err)
		}
		handle.Destroy()
		if err := db.Flush(); err != nil {
			t.Fatalf("Flush(): %v", err)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for budget.Used() < 2*64<<10 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if budget.Used() < 2*64<<10 {
		t.Fatalf("two databases holding 64 KiB each publish %d bytes", budget.Used())
	}
	for _, db := range dbs {
		if err := db.Shutdown(); err != nil {
			t.Fatalf("Shutdown(): %v", err)
		}
		db.Destroy()
	}
	if budget.Used() != 0 {
		t.Fatalf("closed databases still hold %d bytes of the budget", budget.Used())
	}

	small, err := slatedb.NewUnflushedBudget(1 << 20)
	if err != nil {
		t.Fatalf("NewUnflushedBudget(): %v", err)
	}
	defer small.Destroy()
	builder := slatedb.NewDbBuilder("budget-small", store)
	defer builder.Destroy()
	if err := builder.WithUnflushedBudget(small); err != nil {
		t.Fatalf("WithUnflushedBudget(): %v", err)
	}
	db, err := builder.Build()
	if db != nil {
		db.Destroy()
	}
	if !errors.Is(err, slatedb.ErrErrorInvalid) {
		t.Fatalf("Build() on a budget under max_unflushed_bytes: got %v, want invalid", err)
	}
}
