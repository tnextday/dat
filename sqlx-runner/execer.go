package runner

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"gopkg.in/mgutz/dat.v1"
)

// Execer executes queries against a database.
type Execer struct {
	database
	builder dat.Builder

	cacheID         string
	cacheTTL        time.Duration
	cacheInvalidate bool

	timeout time.Duration
	// uuid is inserted into the SQL for the query to be searched
	// in pg_stat_activity. This only happens when timeout is set.
	queryID string
}

// NewExecer creates a new instance of Execer.
func NewExecer(database database, builder dat.Builder) *Execer {
	return &Execer{
		database: database,
		builder:  builder,
	}
}

// Cache caches the results of queries for Select and SelectDoc.
func (ex *Execer) Cache(id string, ttl time.Duration, invalidate bool) dat.Execer {
	ex.cacheID = id
	ex.cacheTTL = ttl
	ex.cacheInvalidate = invalidate
	return ex
}

// Timeout sets the timeout for current query.
func (ex *Execer) Timeout(timeout time.Duration) dat.Execer {
	ex.timeout = timeout
	if ex.timeout > 0 {
		ex.queryID = uuid()
	} else {
		ex.queryID = ""
	}
	return ex
}

const queryIDPrefix = "--dat:qid="

func datQueryID(id string) string {
	return fmt.Sprintf("--dat:qid=%s", id)
}

func prependDatQueryID(sql string, id string) string {
	return fmt.Sprintf("%s\n%s", datQueryID(id), sql)
}

// Cancel cancels last query with a queryID. If queryID was not set then
// ErrInvalidOperation is returned.
func (ex *Execer) Cancel() error {
	if ex.queryID == "" {
		return dat.ErrInvalidOperation
	}

	q := fmt.Sprintf(`
	SELECT pg_cancel_backend(psa.pid)
	FROM (
		SELECT pid
		FROM pg_stat_activity
		WHERE query
		LIKE '%s%%'
	) psa`, datQueryID(ex.queryID))

	_, err := execSQL(ex, q, nil)
	return err
}

// Interpolate tells the associated builder to interpolate itself.
func (ex *Execer) Interpolate() (string, []interface{}, error) {
	sql, args, err := ex.builder.Interpolate()
	if ex.timeout > 0 {
		sql = prependDatQueryID(sql, ex.queryID)
	}
	return sql, args, err
}

// Exec executes a builder's query.
func (ex *Execer) Exec() (*dat.Result, error) {
	res, err := exec(ex)
	if err != nil {
		return nil, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	return &dat.Result{RowsAffected: rowsAffected}, nil
}

// Queryx executes builder's query and returns rows.
func (ex *Execer) Queryx() (*sqlx.Rows, error) {
	return query(ex)
}

// QueryScalar executes builder's query and scans returned row into destinations.
func (ex *Execer) QueryScalar(destinations ...interface{}) error {
	return queryScalar(ex, destinations...)
}

// QuerySlice executes builder's query and builds a slice of values from each row, where
// each row only has one column.
func (ex *Execer) QuerySlice(dest interface{}) error {
	return querySlice(ex, dest)
}

// QueryStruct executes builders' query and scans the result row into dest.
func (ex *Execer) QueryStruct(dest interface{}) error {
	if _, ok := ex.builder.(*dat.SelectDocBuilder); ok {
		err := queryJSONStruct(ex, dest)
		return err
	}
	return queryStruct(ex, dest)
}

// QueryStructs executes builders' query and scans each row as an item in a slice of structs.
func (ex *Execer) QueryStructs(dest interface{}) error {
	if _, ok := ex.builder.(*dat.SelectDocBuilder); ok {
		err := queryJSONStructs(ex, dest)
		return err
	}

	return queryStructs(ex, dest)
}

// QueryObject wraps the builder's query within a `to_json` then executes and unmarshals
// the result into dest.
func (ex *Execer) QueryObject(dest interface{}) error {
	if _, ok := ex.builder.(*dat.SelectDocBuilder); ok {
		b, err := queryJSONBlob(ex, false)
		if err != nil {
			return err
		}
		if b == nil {
			return nil
		}
		return json.Unmarshal(b, dest)
	}

	return queryObject(ex, dest)
}

// QueryJSON wraps the builder's query within a `to_json` then executes and returns
// the JSON []byte representation.
func (ex *Execer) QueryJSON() ([]byte, error) {
	if _, ok := ex.builder.(*dat.SelectDocBuilder); ok {
		return queryJSONBlob(ex, false)
	}

	return queryJSON(ex)
}
