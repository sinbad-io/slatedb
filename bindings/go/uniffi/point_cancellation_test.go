package slatedb_test

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	slatedb "slatedb.io/slatedb-go/uniffi"
)

var pointTokenSurfaces = []string{"writer", "writer_snapshot", "reader", "reader_snapshot"}

type pointTokenResult struct {
	value *[]byte
	err   error
}

type pointTokenHold struct {
	entered chan struct{}
	release chan struct{}
	left    chan struct{}
	once    sync.Once
	fail    bool
	primed  bool
	failed  atomic.Int64
}

func (h *pointTokenHold) unblock() { h.once.Do(func() { close(h.release) }) }

type pointTokenTransform struct {
	mu      sync.Mutex
	next    *pointTokenHold
	decodes atomic.Int64
}

func (*pointTokenTransform) Encode(data []byte) ([]byte, error) { return bytes.Clone(data), nil }

func (f *pointTokenTransform) Decode(data []byte) ([]byte, error) {
	f.decodes.Add(1)
	f.mu.Lock()
	h := f.next
	if h != nil && h.fail && !h.primed {
		// The existing SST validation path retries one transform error.
		// Hold the second attempt, whose error reaches the caller.
		h.primed = true
		f.mu.Unlock()
		h.failed.Add(1)
		return nil, slatedb.NewBlockTransformerCallbackErrorFailed("original point decoder refused")
	}
	f.next = nil
	f.mu.Unlock()
	if h != nil {
		close(h.entered)
		<-h.release
		defer close(h.left)
		if h.fail {
			h.failed.Add(1)
			return nil, slatedb.NewBlockTransformerCallbackErrorFailed("original point decoder refused")
		}
	}
	return bytes.Clone(data), nil
}

func (f *pointTokenTransform) arm(fail bool) *pointTokenHold {
	h := &pointTokenHold{entered: make(chan struct{}), release: make(chan struct{}), left: make(chan struct{}), fail: fail}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.next != nil {
		panic("point decoder already armed")
	}
	f.next = h
	return h
}

func pointTokenOptions(token *slatedb.CancellationToken) slatedb.ReadOptions {
	var option **slatedb.CancellationToken
	if token != nil {
		option = &token
	}
	return slatedb.ReadOptions{DurabilityFilter: slatedb.DurabilityLevelRemote, CacheBlocks: false, CancellationToken: option}
}

func pointTokenOpen(t *testing.T, surface string) (func(*slatedb.CancellationToken) pointTokenResult, *pointTokenTransform) {
	t.Helper()
	obj := newMemoryStore(t)
	transform := &pointTokenTransform{}
	w := openTestDB(t, obj, func(t *testing.T, b *slatedb.DbBuilder) {
		if err := b.WithDbCacheDisabled(); err != nil {
			t.Fatal(err)
		}
		if err := b.WithBlockTransformer(transform); err != nil {
			t.Fatal(err)
		}
	})
	h, err := w.db.Put([]byte("point/key"), []byte("point value"))
	if err != nil {
		t.Fatal(err)
	}
	awaitDurable(t, trackWriteHandle(t, h))
	if err := w.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeMemTable}); err != nil {
		t.Fatal(err)
	}
	get := w.db.GetWithOptions
	switch surface {
	case "writer":
	case "writer_snapshot":
		sn, err := w.db.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(sn.Destroy)
		get = sn.GetWithOptions
	case "reader", "reader_snapshot":
		r := openTestReader(t, obj, func(t *testing.T, b *slatedb.DbReaderBuilder) {
			if err := b.WithReaderMode(slatedb.ReaderModeFollowLatest{}); err != nil {
				t.Fatal(err)
			}
			if err := b.WithDbCacheDisabled(); err != nil {
				t.Fatal(err)
			}
			if err := b.WithBlockTransformer(transform); err != nil {
				t.Fatal(err)
			}
		})
		get = r.reader.GetWithOptions
		if surface == "reader_snapshot" {
			sn, err := r.reader.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(sn.Destroy)
			get = sn.GetWithOptions
		}
	default:
		t.Fatalf("unknown point surface %q", surface)
	}
	return func(token *slatedb.CancellationToken) pointTokenResult {
		v, err := get([]byte("point/key"), pointTokenOptions(token))
		return pointTokenResult{v, err}
	}, transform
}

func pointTokenPositive(t *testing.T, read func(*slatedb.CancellationToken) pointTokenResult) {
	t.Helper()
	r := read(nil)
	if r.err != nil || valueOf(r.value) != "point value" {
		t.Fatalf("original point view failed after refusal: %q, %v", valueOf(r.value), r.err)
	}
}

func pointTokenCancelled(t *testing.T, r pointTokenResult) {
	t.Helper()
	if r.value != nil || !errors.Is(r.err, slatedb.ErrErrorCancelled) {
		t.Errorf("point token returned bytes or lost cancellation: %q, %v", valueOf(r.value), r.err)
	}
}

func TestPointTokenRefusesBeforeDecode(t *testing.T) {
	for _, surface := range pointTokenSurfaces {
		t.Run(surface, func(t *testing.T) {
			read, transform := pointTokenOpen(t, surface)
			token := slatedb.NewCancellationToken()
			t.Cleanup(token.Destroy)
			token.Cancel()
			before := transform.decodes.Load()
			r := read(token)
			after := transform.decodes.Load()
			pointTokenPositive(t, read)
			pointTokenCancelled(t, r)
			if after != before {
				t.Errorf("cancelled read entered %d original decoders", after-before)
			}
		})
	}
}

func TestPointTokenJoinsOriginalDecoder(t *testing.T) {
	pointTokenHeldCases(t, false)
}

func TestPointTokenPreservesOriginalDecoderError(t *testing.T) {
	pointTokenHeldCases(t, true)
}

func pointTokenHeldCases(t *testing.T, fail bool) {
	t.Helper()
	for _, surface := range pointTokenSurfaces {
		t.Run(surface, func(t *testing.T) {
			read, transform := pointTokenOpen(t, surface)
			token := slatedb.NewCancellationToken()
			t.Cleanup(token.Destroy)
			hold := transform.arm(fail)
			finished := make(chan struct{})
			var result pointTokenResult
			go func() {
				defer close(finished)
				result = read(token)
			}()
			// Cleanup releases and joins the original call before native handles.
			t.Cleanup(func() { hold.unblock(); <-finished })
			timer := time.NewTimer(3 * time.Second)
			defer timer.Stop()
			select {
			case <-hold.entered:
			case <-finished:
				t.Fatalf("point fixture returned before its decoder: %q, %v", valueOf(result.value), result.err)
			case <-timer.C:
				t.Fatal("point fixture did not enter its original decoder")
			}
			token.Cancel()
			early := false
			joinWindow := time.NewTimer(50 * time.Millisecond)
			select {
			case <-finished:
				early = true
			case <-joinWindow.C:
			}
			joinWindow.Stop()
			hold.unblock()
			<-finished
			<-hold.left
			pointTokenPositive(t, read)
			if early {
				t.Error("point read returned before its original decoder joined")
			}
			if fail {
				if got := hold.failed.Load(); got != 2 {
					t.Fatalf("point error fixture used %d failed decoders, want both validation attempts", got)
				}
				if result.value != nil || !errors.Is(result.err, slatedb.ErrErrorData) || !strings.Contains(result.err.Error(), "error transforming block") {
					t.Errorf("point cancellation hid original decoder error: %q, %v", valueOf(result.value), result.err)
				}
			} else {
				pointTokenCancelled(t, result)
			}
		})
	}
}
