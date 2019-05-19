package mdb

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

type MemTokens struct {
	Id        int
	CsrfToken string
	CreatedAt time.Time
}

type MemTokensStore struct {
	sync.RWMutex
	db      *sql.DB
	store   []*MemTokens
	deleted []bool

	csrfTokenIndex *StringUniqueIndex
	executor       *AsyncQueryExecutor
}

func NewMemTokensStore(db *sql.DB) *MemTokensStore {
	initialSize := 100000
	store := make([]*MemTokens, 0, initialSize)
	deleted := make([]bool, 0, initialSize)

	// id = 0 is unavailable
	store = append(store, nil)
	deleted = append(deleted, true)

	rows, err := db.Query("SELECT * FROM tokens")
	if err != nil {
		log.Fatal(err)
	}

	csrfTokenIndex := NewStringUniqueIndex()

	for rows.Next() {
		e := MemTokens{}
		err := rows.Scan(&e.Id, &e.CsrfToken, &e.CreatedAt)
		if err != nil {
			log.Fatal(err)
		}
		for e.Id >= len(store) {
			store = append(store, nil)
			deleted = append(deleted, true)
		}

		csrfTokenIndex.Insert(e.CsrfToken, e.Id)
	}

	rows.Close()

	return &MemTokensStore{
		RWMutex:  sync.RWMutex{},
		db:       db,
		store:    store,
		deleted:  deleted,
		executor: NewAsyncQueryExecutor(db),

		csrfTokenIndex: csrfTokenIndex,
	}
}

func (s *MemTokensStore) Insert(e *MemTokens) (int, error) {
	s.Lock()
	defer s.Unlock()

	if _, found := s.csrfTokenIndex.Find(e.CsrfToken); found {
		return 0, errors.New("failed to insert: Same CsrfToken found")
	}

	ne := *e
	ne.Id = len(s.store)
	s.store = append(s.store, &ne)
	s.deleted = append(s.deleted, false)

	if err := s.csrfTokenIndex.Insert(ne.CsrfToken, ne.Id); err != nil {
		log.Fatal(err)
	}

	s.executor.AddQuery("INSERT INTO tokens VALUES(?,?,?)",
		[]interface{}{ne.Id, ne.CsrfToken, ne.CreatedAt})

	return ne.Id, nil
}

func (s *MemTokensStore) Delete(id int) error {
	s.Lock()
	defer s.Unlock()

	if id <= 0 {
		return fmt.Errorf("id should be positive : %d", id)
	}
	if id >= len(s.store) {
		return fmt.Errorf("id is larger than store size : %d > %d", id, len(s.store))
	}
	if s.deleted[id] {
		return fmt.Errorf("id(%d) is already deleted", id)
	}

	s.deleted[id] = true

	s.executor.AddQuery("DELETE FROM Tokens WHERE id = ?", []interface{}{id})
	return nil
}

func (s *MemTokensStore) SelectFromId(id int) (*MemTokens, error) {
	s.RLock()
	defer s.RUnlock()

	if id <= 0 {
		return nil, fmt.Errorf("id should be positive : %d", id)
	}
	if id >= len(s.store) {
		return nil, fmt.Errorf("id is larger than store size : %d > %d", id, len(s.store))
	}
	if s.deleted[id] {
		return nil, fmt.Errorf("id(%d) is deleted", id)
	}

	ret := *s.store[id]
	return &ret, nil
}

func (s *MemTokensStore) SelectFromCsrfToken(CsrfToken string) (*MemTokens, error) {
	s.RLock()
	defer s.RUnlock()

	id, found := s.csrfTokenIndex.Find(CsrfToken)
	if !found {
		return nil, fmt.Errorf("CsrfToken(%s) is not found", CsrfToken)
	}

	if id <= 0 || id >= len(s.store) || s.deleted[id] {
		log.Fatal("internal error.")
	}

	ret := *s.store[id]
	return &ret, nil
}
