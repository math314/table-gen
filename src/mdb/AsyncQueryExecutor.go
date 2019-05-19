package mdb

import (
	"database/sql"
	"log"
)

type Executor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

type AsyncQueryExecutor struct {
	db       Executor
	dbOpChan chan<- DBOperatorEntry
}

func NewAsyncQueryExecutor(db Executor) *AsyncQueryExecutor {
	dbOpChan := make(chan DBOperatorEntry, 100)
	startDBOpfunc(db, dbOpChan)
	return &AsyncQueryExecutor{
		db:       db,
		dbOpChan: dbOpChan,
	}
}

func (e *AsyncQueryExecutor) Close() {
	close(e.dbOpChan)
}

func (e *AsyncQueryExecutor) AddQuery(query string, args ...interface{}) {
	e.dbOpChan <- DBOperatorEntry{query, args}
}

type DBOperatorEntry struct {
	query string
	args  []interface{}
}

func startDBOpfunc(db Executor, dbOps <-chan DBOperatorEntry) {
	go func() {
		for val := range dbOps {
			log.Printf("query = %s, args = %v", val.query, val.args)
			_, err := db.Exec(val.query, val.args...)
			if err != nil {
				log.Print(err)
			}
		}
	}()
}
