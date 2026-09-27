package slatedb_test

import (
	"testing"

	slatedb "slatedb.io/slatedb-go/uniffi"
)

// A block cache policy reaches the engine: a flush under an empty flush
// policy caches nothing, where the default caches the flushed blocks.
func TestAnEmptyFlushPolicyCachesNothingOnFlush(t *testing.T) {
	weightAfterFlush := func(policy *slatedb.BlockCachePolicy) uint64 {
		store := newMemoryStore(t)
		cache := newBoundedCache(t, 64<<20)
		dbHandle := openTestDB(t, store, func(t *testing.T, builder *slatedb.DbBuilder) {
			if err := builder.WithDbCache(cache, 7); err != nil {
				t.Fatalf("WithDbCache(): %v", err)
			}
			if policy != nil {
				if err := builder.WithBlockCachePolicy(*policy); err != nil {
					t.Fatalf("WithBlockCachePolicy(): %v", err)
				}
			}
		})
		if _, err := dbHandle.db.Put([]byte("k"), []byte("v")); err != nil {
			t.Fatalf("Put(): %v", err)
		}
		if err := dbHandle.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeMemTable}); err != nil {
			t.Fatalf("FlushWithOptions(): %v", err)
		}
		weight := cache.IndexedWeight()
		if weight == nil {
			t.Fatal("a bounded cache reports its indexed weight")
		}
		if err := dbHandle.db.Shutdown(); err != nil {
			t.Fatalf("Shutdown(): %v", err)
		}
		dbHandle.open = false
		return *weight
	}
	if got := weightAfterFlush(nil); got == 0 {
		t.Fatal("the default policy cached nothing on flush")
	}
	empty := slatedb.BlockCachePolicy{FlushTargets: []slatedb.CacheTarget{}, CompactionOutputTargets: []slatedb.CacheTarget{slatedb.CacheTargetIndex{}}}
	if got := weightAfterFlush(&empty); got != 0 {
		t.Fatalf("an empty flush policy cached %d bytes", got)
	}
}
