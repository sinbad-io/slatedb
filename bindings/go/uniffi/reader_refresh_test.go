package slatedb_test

import (
	"testing"

	slatedb "slatedb.io/slatedb-go/uniffi"
)

func valueOf(got *[]byte) string {
	if got == nil {
		return "<nil>"
	}
	return string(*got)
}

// A reader polling once an hour does not see a durable write until it is
// asked to refresh; Refresh returns once the write is installed.
func TestReaderRefreshInstallsADurableWriteAheadOfThePoll(t *testing.T) {
	store := newMemoryStore(t)
	dbHandle := openTestDB(t, store, nil)
	if _, err := dbHandle.db.Put([]byte("k"), []byte("before")); err != nil {
		t.Fatalf("Put(): %v", err)
	}
	if err := dbHandle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeMemTable}); err != nil {
		t.Fatalf("FlushWithOptions(): %v", err)
	}
	reader := openTestReader(t, store, func(t *testing.T, builder *slatedb.DbReaderBuilder) {
		t.Helper()
		if err := builder.WithReaderMode(slatedb.ReaderModeFollowLatest{}); err != nil {
			t.Fatalf("WithReaderMode(): %v", err)
		}
		opts := slatedb.ReaderOptions{ManifestPollIntervalMs: 3_600_000, CheckpointLifetimeMs: 600_000, MaxMemtableBytes: 64 << 20}
		if err := builder.WithOptions(opts); err != nil {
			t.Fatalf("WithOptions(): %v", err)
		}
	})
	if got, err := reader.reader.Get([]byte("k")); err != nil || valueOf(got) != "before" {
		t.Fatalf("Get() before the write = %q, %v", valueOf(got), err)
	}
	handle, err := dbHandle.db.Put([]byte("k"), []byte("after"))
	if err != nil {
		t.Fatalf("Put(): %v", err)
	}
	if err := handle.AwaitDurable(); err != nil {
		t.Fatalf("AwaitDurable(): %v", err)
	}
	handle.Destroy()
	if got, err := reader.reader.Get([]byte("k")); err != nil || valueOf(got) != "before" {
		t.Fatalf("Get() after the write and before Refresh = %q, %v; the hour-long poll must not have run", valueOf(got), err)
	}
	if err := reader.reader.Refresh(); err != nil {
		t.Fatalf("Refresh(): %v", err)
	}
	if got, err := reader.reader.Get([]byte("k")); err != nil || valueOf(got) != "after" {
		t.Fatalf("Get() after Refresh = %q, %v; want after", valueOf(got), err)
	}
}

// The reader's object-store cache options reach the engine and a reader with
// a disk cache under a fresh root serves its reads.
func TestReaderOptionsCarryTheObjectStoreCache(t *testing.T) {
	store := newMemoryStore(t)
	dbHandle := openTestDB(t, store, nil)
	if _, err := dbHandle.db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put(): %v", err)
	}
	if err := dbHandle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeMemTable}); err != nil {
		t.Fatalf("FlushWithOptions(): %v", err)
	}
	root := t.TempDir()
	maxBytes := uint64(64 << 20)
	scan := uint64(60_000)
	level := slatedb.PreloadLevelL0Sst
	cache := slatedb.ObjectStoreCacheOptions{
		RootFolder: &root, MaxCacheSizeBytes: &maxBytes, PartSizeBytes: 1 << 20,
		CacheOnFlush: false, CacheOnCompaction: false, PreloadDiskCacheOnStartup: &level,
		ScanIntervalMs: &scan, MaxOpenFileHandles: 16,
	}
	reader := openTestReader(t, store, func(t *testing.T, builder *slatedb.DbReaderBuilder) {
		t.Helper()
		if err := builder.WithReaderMode(slatedb.ReaderModeFollowLatest{}); err != nil {
			t.Fatalf("WithReaderMode(): %v", err)
		}
		opts := slatedb.ReaderOptions{ManifestPollIntervalMs: 100, CheckpointLifetimeMs: 600_000, MaxMemtableBytes: 64 << 20, ObjectStoreCacheOptions: &cache}
		if err := builder.WithOptions(opts); err != nil {
			t.Fatalf("WithOptions(cache): %v", err)
		}
	})
	if got, err := reader.reader.Get([]byte("k")); err != nil || valueOf(got) != "v" {
		t.Fatalf("Get() through a reader with a disk cache = %q, %v", valueOf(got), err)
	}

}
