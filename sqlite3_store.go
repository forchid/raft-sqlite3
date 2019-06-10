package raftsqlite3

import (
	"errors"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"time"
	
	_ "github.com/mattn/go-sqlite3"
	"github.com/hashicorp/raft"
)

const (
	// Table names we perform transactions in
	dbLogs = "logs"
	dbConf = "conf"
)

var (
	// An error indicating a given key does not exist
	ErrKeyNotFound = errors.New("not found")
)

// Sqlite3Store provides access to sqlite3 for Raft to store and retrieve
// log entries. It also provides key/value storage, and can be used as
// a LogStore and StableStore.
type Sqlite3Store struct {
	// db is the underlying handle to the db.
	db *sql.DB
}

func NewSqlite3Store(dataSourceName string) (*Sqlite3Store, error) {
	return New(dataSourceName)
}

// New uses the supplied dataSourceName to open the sqlite3 and prepare it for use as a raft backend.
func New(dataSourceName string) (*Sqlite3Store, error) {
	if strings.Index(dataSourceName, "?") == -1 {
		const extra = "_busy_timeout=30000&_journal_mode=WAL"//"&_synchronous=NORMAL"
		dataSourceName = fmt.Sprintf("%s?%s", dataSourceName, extra)
	}
	// Try to open and connect
	db, err := sql.Open("sqlite3", dataSourceName)
	if err != nil {
		return nil, err
	}

	// Create the new store
	store := &Sqlite3Store{
		db: db,
	}

	// If the store was opened read-only, don't try and create tables
	readOnly, err := store.readOnly()
	if err != nil {
		store.Close()
		return nil, err
	}
	if !readOnly {
		// Set up our buckets
		if err := store.initialize(); err != nil {
			store.Close()
			return nil, err
		}
	}
	
	return store, nil
}

// readOnly returns true if the open store is in query_only mode [this can be 
// useful to tools that want to examine the log]
func (s *Sqlite3Store) readOnly() (bool, error) {
	readOnly := true
	row := s.db.QueryRow("pragma query_only")
	err := row.Scan(&readOnly)
	return readOnly, err
}

// initialize is used to set up all of the tables.
func (s *Sqlite3Store) initialize() error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func(){
		if err != nil {
			tx.Rollback()
			return
		}
		if m := recover(); m != nil{
			tx.Rollback()
			err = fmt.Errorf("%s", m)
		}
	}()

	// Create all the tables
	query := fmt.Sprintf("create table if not exists %s(id integer not null primary key, value blob)", dbLogs)
	if _, err := tx.Exec(query); err != nil {
		return err
	}
	query  = fmt.Sprintf("create table if not exists %s(id blob not null primary key, value blob)", dbConf)
	if _, err := tx.Exec(query); err != nil {
		return err
	}

	return tx.Commit()
}

// Close is used to gracefully close the DB connection.
func (s *Sqlite3Store) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

// FirstIndex returns the first known index from the Raft log.
func (s *Sqlite3Store) FirstIndex() (uint64, error) {
	query  := fmt.Sprintf("select id from %s order by id asc limit 1", dbLogs)
	stmt, err := s.db.Prepare(query)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	
	var first uint64
	row := stmt.QueryRow()
	err = row.Scan(&first)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	
	return first, err
}

// LastIndex returns the last known index from the Raft log.
func (s *Sqlite3Store) LastIndex() (uint64, error) {
	query  := fmt.Sprintf("select id from %s order by id desc limit 1", dbLogs)
	stmt, err := s.db.Prepare(query)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	
	var last uint64
	row := stmt.QueryRow()
	err = row.Scan(&last)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	
	return last, err
}

// GetLog is used to retrieve a log from sqlite3 at a given index.
func (s *Sqlite3Store) GetLog(idx uint64, log *raft.Log) error {
	query  := fmt.Sprintf("select value from %s where id = ?", dbLogs)
	stmt, err := s.db.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()
	
	var val []byte
	row := stmt.QueryRow(idx)
	err = row.Scan(&val)
	if err == sql.ErrNoRows {
		return raft.ErrLogNotFound
	}
	
	return decodeMsgPack(val, log)
}

// StoreLog is used to store a single raft log
func (s *Sqlite3Store) StoreLog(log *raft.Log) error {
	return s.StoreLogs([]*raft.Log{log})
}

// StoreLogs is used to store a set of raft logs
func (s *Sqlite3Store) StoreLogs(logs []*raft.Log) (err error) {
	// Try to do when busy
	// @since 2019-06-11 little-pan
	for {
		if err = s.doStoreLogs(logs); err != nil {
			if waitIfBusy(err) {
				continue
			}
			return err
		}
		
		return nil
	}
}

func (s *Sqlite3Store) doStoreLogs(logs []*raft.Log) (err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return
	}
	defer func(){
		if err != nil {
			tx.Rollback()
			return
		}
		if m := recover(); m != nil{
			tx.Rollback()
			err = fmt.Errorf("%s", m)
		}
	}()

	query := fmt.Sprintf("insert into %s(id, value)values(?, ?)", dbLogs)
	stmt, err := tx.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()
	
	for _, log := range logs {
		key := log.Index
		val, err := encodeMsgPack(log)
		if err != nil {
			return err
		}
		if _, err = stmt.Exec(key, val.Bytes()) ; err != nil {
			return err
		}
	}

	return tx.Commit()
}

// DeleteRange is used to delete logs within a given range inclusively.
func (s *Sqlite3Store) DeleteRange(min, max uint64) error {
	// Delete range by batch for database locked issue
	// @since 2019-06-11 little-pan
	a, batch := min, uint64(999)
	b := uint64(math.Min(float64(a + batch), float64(max - a + uint64(1))))
	for {
		if err := s.doDeleteRange(a, b); err != nil {
			if waitIfBusy(err) {
				continue
			}
			return err
		}
		
		a = b + uint64(1)
		if a > max {
			return nil
		}
		
		b += uint64(math.Min(float64(a + batch), float64(max - a + uint64(1))))
	}
}

func waitIfBusy(err error) bool {
	if strings.Index(err.Error(), "database is locked") != -1 {
		// Try to do again when busy
		time.Sleep(250 * time.Millisecond)
		return true
	}
	
	return false
}

func (s *Sqlite3Store) doDeleteRange(min, max uint64) error {
	query := fmt.Sprintf("delete from %s where id >= ? and id <= ?", dbLogs)
	stmt, err := s.db.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()
	
	_, err = stmt.Exec(min, max)
	return err
}

// Set is used to set a key/value set outside of the raft log
func (s *Sqlite3Store) Set(k, v []byte) error {
	query := fmt.Sprintf("replace into %s(id, value)values(?, ?)", dbConf)
	stmt, err := s.db.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()
	
	if _, err := stmt.Exec(k, v); err != nil {
		return err
	}
	
	return nil
}

// Get is used to retrieve a value from the k/v store by key
func (s *Sqlite3Store) Get(k []byte) ([]byte, error) {
	query := fmt.Sprintf("select value from %s where id = ?", dbConf)
	stmt, err := s.db.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	
	var val []byte
	row := stmt.QueryRow(k)
	err = row.Scan(&val)
	if err == sql.ErrNoRows {
		return  nil, ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}
	
	return append([]byte(nil), val...), nil
}

// SetUint64 is like Set, but handles uint64 values
func (s *Sqlite3Store) SetUint64(key []byte, val uint64) error {
	return s.Set(key, uint64ToBytes(val))
}

// GetUint64 is like Get, but handles uint64 values
func (s *Sqlite3Store) GetUint64(key []byte) (uint64, error) {
	val, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(val), nil
}
