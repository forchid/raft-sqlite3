package raftsqlite3

import (
	"bytes"
	"database/sql"
	"io/ioutil"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/hashicorp/raft"
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

func testRaftLog(idx uint64, data string) *raft.Log {
	return &raft.Log{
		Data:  []byte(data),
		Index: idx,
	}
}

func TestSqlite3Store_Implements(t *testing.T) {
	var store interface{} = &raftsqlite3.Sqlite3Store{}
	if _, ok := store.(raft.StableStore); !ok {
		t.Fatalf("Sqlite3Store does not implement raft.StableStore")
	}
	if _, ok := store.(raft.LogStore); !ok {
		t.Fatalf("Sqlite3Store does not implement raft.LogStore")
	}
}

func TestSqlite3ReadOnly(t *testing.T) {
	fh, err := ioutil.TempFile("", "sqlite3.db")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer os.Remove(fh.Name())
	
	store, err := raftsqlite3.NewSqlite3Store(fh.Name())
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	// Create the log
	log := &raft.Log{
		Data:  []byte("log1"),
		Index: 1,
	}
	// Attempt to store the log
	if err := store.StoreLog(log); err != nil {
		t.Fatalf("err: %s", err)
	}
	store.Close()
	
	dsn := fmt.Sprintf("%s?_query_only=true", fh.Name())
	roStore, err := raftsqlite3.New(dsn)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer roStore.Close()
	result := new(raft.Log)
	if err := roStore.GetLog(1, result); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the log comes back the same
	if !reflect.DeepEqual(log, result) {
		t.Errorf("bad: %v", result)
	}
	// Attempt to store the log, should fail on a read-only store
	err = roStore.StoreLog(log)
	if err != nil {
		e, ok := err.(sqlite3.Error)
		if !ok || e.Code != sqlite3.ErrReadonly {
			t.Errorf("expecting error sqlite3.ErrReadonly, but got %v", err)
		}
		return
	}
	
	t.Errorf("expecting error sqlite3.ErrReadonly, but got %v", err)
}

func TestNewSqlite3Store(t *testing.T) {
	fh, err := ioutil.TempFile("", "sqlite3.db")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	os.Remove(fh.Name())
	defer os.Remove(fh.Name())

	// Successfully creates and returns a store
	store, err := raftsqlite3.NewSqlite3Store(fh.Name())
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the file was created
	if _, err := os.Stat(fh.Name()); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Close the store so we can open again
	if err := store.Close(); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure our tables were created
	db, err := sql.Open("sqlite3", fh.Name())
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer db.Close()
	
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer tx.Rollback()
	
	var n int
	query := fmt.Sprintf("select count(*) from %s", "logs")
	row := tx.QueryRow(query)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("bad: %v", err)
	}
	
	query = fmt.Sprintf("select count(*) from %s", "conf")
	row = tx.QueryRow(query)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("bad: %v", err)
	}
}

func TestSqlite3Store_FirstIndex(t *testing.T) {
	store, path := testSqlite3Store(t)
	defer store.Close()
	defer os.Remove(path)

	// Should get 0 index on empty log
	idx, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad: %v", idx)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Fetch the first Raft index
	idx, err = store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 1 {
		t.Fatalf("bad: %d", idx)
	}
}

func TestSqlite3Store_LastIndex(t *testing.T) {
	store, path := testSqlite3Store(t)
	defer store.Close()
	defer os.Remove(path)

	// Should get 0 index on empty log
	idx, err := store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad: %v", idx)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Fetch the last Raft index
	idx, err = store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 3 {
		t.Fatalf("bad: %d", idx)
	}
}

func TestSqlite3Store_GetLog(t *testing.T) {
	store, path := testSqlite3Store(t)
	defer store.Close()
	defer os.Remove(path)

	log := new(raft.Log)

	// Should return an error on non-existent log
	if err := store.GetLog(1, log); err != raft.ErrLogNotFound {
		t.Fatalf("expected raft log not found error, got: %v", err)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Should return the proper log
	if err := store.GetLog(2, log); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(log, logs[1]) {
		t.Fatalf("bad: %#v", log)
	}
}

func TestSqlite3Store_SetLog(t *testing.T) {
	store, path := testSqlite3Store(t)
	defer store.Close()
	defer os.Remove(path)

	// Create the log
	log := &raft.Log{
		Data:  []byte("log1"),
		Index: 1,
	}

	// Attempt to store the log
	if err := store.StoreLog(log); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Retrieve the log again
	result := new(raft.Log)
	if err := store.GetLog(1, result); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the log comes back the same
	if !reflect.DeepEqual(log, result) {
		t.Fatalf("bad: %v", result)
	}
}

func TestSqlite3Store_SetLogs(t *testing.T) {
	store, path := testSqlite3Store(t)
	defer store.Close()
	defer os.Remove(path)

	// Create a set of logs
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
	}

	// Attempt to store the logs
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure we stored them all
	result1, result2 := new(raft.Log), new(raft.Log)
	if err := store.GetLog(1, result1); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(logs[0], result1) {
		t.Fatalf("bad: %#v", result1)
	}
	if err := store.GetLog(2, result2); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(logs[1], result2) {
		t.Fatalf("bad: %#v", result2)
	}
}

func TestSqlite3Store_DeleteRange(t *testing.T) {
	store, path := testSqlite3Store(t)
	defer store.Close()
	defer os.Remove(path)

	// Create a set of logs
	log1 := testRaftLog(1, "log1")
	log2 := testRaftLog(2, "log2")
	log3 := testRaftLog(3, "log3")
	logs := []*raft.Log{log1, log2, log3}

	// Attempt to store the logs
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Attempt to delete a range of logs
	if err := store.DeleteRange(1, 2); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the logs were deleted
	if err := store.GetLog(1, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log1")
	}
	if err := store.GetLog(2, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log2")
	}
}

func TestSqlite3Store_Set_Get(t *testing.T) {
	store, path := testSqlite3Store(t)
	defer store.Close()
	defer os.Remove(path)

	// Returns error on non-existent key
	if _, err := store.Get([]byte("bad")); err != raftsqlite3.ErrKeyNotFound {
		t.Fatalf("expected not found error, got: %q", err)
	}

	k, v := []byte("hello"), []byte("world")

	// Try to set a k/v pair
	if err := store.Set(k, v); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Try to read it back
	val, err := store.Get(k)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if !bytes.Equal(val, v) {
		t.Fatalf("bad: %v", val)
	}
}

func TestSqlite3Store_SetUint64_GetUint64(t *testing.T) {
	store, path := testSqlite3Store(t)
	defer store.Close()
	defer os.Remove(path)

	// Returns error on non-existent key
	if _, err := store.GetUint64([]byte("bad")); err != raftsqlite3.ErrKeyNotFound {
		t.Fatalf("expected not found error, got: %q", err)
	}

	k, v := []byte("abc"), uint64(123)

	// Attempt to set the k/v pair
	if err := store.SetUint64(k, v); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Read back the value
	val, err := store.GetUint64(k)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if val != v {
		t.Fatalf("bad: %v", val)
	}
}
