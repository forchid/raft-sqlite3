package raftsqlite3

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/hashicorp/raft/bench"
	"github.com/little-pan/raft-sqlite3"
)

func testSqlite3Store(t testing.TB) (*raftsqlite3.Sqlite3Store, string) {
	fh, err := ioutil.TempFile("", "sqlite3.db")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	os.Remove(fh.Name())
	
	// Successfully creates and returns a store
	path := fh.Name()
	store, err := raftsqlite3.NewSqlite3Store(path)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	return store, path
}

func BenchmarkSqlite3Store_FirstIndex(b *testing.B) {
	store, path := testSqlite3Store(b)
	defer store.Close()
	defer os.Remove(path)

	raftbench.FirstIndex(b, store)
}

func BenchmarkSqlite3Store_LastIndex(b *testing.B) {
	store, path := testSqlite3Store(b)
	defer store.Close()
	defer os.Remove(path)

	raftbench.LastIndex(b, store)
}

func BenchmarkSqlite3Store_GetLog(b *testing.B) {
	store, path := testSqlite3Store(b)
	defer store.Close()
	defer os.Remove(path)
	
	raftbench.GetLog(b, store)
}

func BenchmarkSqlite3Store_StoreLog(b *testing.B) {
	store, path := testSqlite3Store(b)
	defer store.Close()
	defer os.Remove(path)

	raftbench.StoreLog(b, store)
}

func BenchmarkSqlite3Store_StoreLogs(b *testing.B) {
	store, path := testSqlite3Store(b)
	defer store.Close()
	defer os.Remove(path)

	raftbench.StoreLogs(b, store)
}

func BenchmarkSqlite3Store_DeleteRange(b *testing.B) {
	store, path := testSqlite3Store(b)
	defer store.Close()
	defer os.Remove(path)

	raftbench.DeleteRange(b, store)
}

func BenchmarkSqlite3Store_Set(b *testing.B) {
	store, path := testSqlite3Store(b)
	defer store.Close()
	defer os.Remove(path)

	raftbench.Set(b, store)
}

func BenchmarkSqlite3Store_Get(b *testing.B) {
	store, path := testSqlite3Store(b)
	defer store.Close()
	defer os.Remove(path)

	raftbench.Get(b, store)
}

func BenchmarkSqlite3Store_SetUint64(b *testing.B) {
	store, path := testSqlite3Store(b)
	defer store.Close()
	defer os.Remove(path)

	raftbench.SetUint64(b, store)
}

func BenchmarkSqlite3Store_GetUint64(b *testing.B) {
	store, path := testSqlite3Store(b)
	defer store.Close()
	defer os.Remove(path)

	raftbench.GetUint64(b, store)
}
