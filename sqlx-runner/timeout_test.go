package runner

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gopkg.in/mgutz/dat.v1"
)

func TestTimeoutExec(t *testing.T) {
	_, err := testDB.SQL("SELECT pg_sleep(1)").Timeout(10 * time.Millisecond).Exec()
	assert.Equal(t, err, dat.ErrTimedout)
}

func TestTimeoutNegative(t *testing.T) {
	// give ample time for the DB to execute
	result, err := testDB.SQL("SELECT pg_sleep(1)").Timeout(3 * time.Second).Exec()
	assert.Equal(t, int64(1), result.RowsAffected)
	assert.Nil(t, err)
}

func TestTimeoutScalar(t *testing.T) {
	var s string
	var n int64
	err := testDB.SQL("SELECT pg_sleep(2) as sleep, 1 as k;").Timeout(10*time.Millisecond).QueryScalar(&s, &n)
	assert.Equal(t, dat.ErrTimedout, err)
}
