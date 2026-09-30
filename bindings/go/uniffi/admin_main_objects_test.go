package slatedb_test

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	slatedb "slatedb.io/slatedb-go/uniffi"
)

func TestAdminListMainObjectsKeepsFileMetadataAndScope(t *testing.T) {
	store, root := newLocalStore(t)
	prefix := root + "/tenant/cell"
	contents := map[string][]byte{
		prefix + "/data/value.sst":                    []byte("original payload"),
		prefix + "/wal/empty.sst":                     nil,
		prefix + "/.deleting":                         []byte("retained intent"),
		root + "/tenant/cell-neighbor/data/value.sst": []byte("neighbor"),
	}
	for name, body := range contents {
		path := "/" + name
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, body, 0o600); err != nil {
			t.Fatal(err)
		}
		modified := time.Unix(1_700_000_000, 123_456_789)
		if err := os.Chtimes(path, modified, modified); err != nil {
			t.Fatal(err)
		}
	}
	builder := slatedb.NewAdminBuilder(prefix, store)
	defer builder.Destroy()
	admin, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Destroy)
	objects, err := admin.ListMainObjects()
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 3 {
		t.Fatalf("objects = %d, want 3", len(objects))
	}
	seen := make(map[string]bool)
	for _, object := range objects {
		body, ok := contents[object.Location]
		if !ok || filepath.Dir(object.Location) == root+"/tenant/cell-neighbor/data" || seen[object.Location] {
			t.Fatalf("unexpected or duplicate object %q", object.Location)
		}
		seen[object.Location] = true
		info, err := os.Stat("/" + object.Location)
		if err != nil {
			t.Fatal(err)
		}
		if object.Size != uint64(len(body)) || object.Size != uint64(info.Size()) || object.LastModifiedSeconds != info.ModTime().Unix() || object.LastModifiedNanos != uint32(info.ModTime().Nanosecond()) {
			t.Fatalf("metadata differs from actual file: %+v, stat=%+v", object, info)
		}
	}
	for name, want := range contents {
		got, err := os.ReadFile("/" + name)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("observation changed %q: bytes=%q error=%v", name, got, err)
		}
	}
	again, err := admin.ListMainObjects()
	sort.Slice(objects, func(i, j int) bool { return objects[i].Location < objects[j].Location })
	sort.Slice(again, func(i, j int) bool { return again[i].Location < again[j].Location })
	if err != nil || !reflect.DeepEqual(again, objects) {
		t.Fatalf("unchanged listing differs: first=%+v second=%+v error=%v", objects, again, err)
	}
}

func TestAdminListMainObjectsEmptyPrefixCreatesNoIntent(t *testing.T) {
	store, root := newLocalStore(t)
	builder := slatedb.NewAdminBuilder(root+"/empty", store)
	defer builder.Destroy()
	admin, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Destroy)
	objects, err := admin.ListMainObjects()
	if err != nil || len(objects) != 0 {
		t.Fatalf("empty listing: objects=%+v error=%v", objects, err)
	}
	entries, err := os.ReadDir("/" + root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("observation created an intent: entries=%+v error=%v", entries, err)
	}
}
