// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package postgresqlreceiver

import (
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errMidStream is the failure database/sql reports from rows.Err() after
// rows.Next() has returned false: a query can fail partway through streaming
// its result set (connection drop, server error), and without consulting
// rows.Err() the helper returns the partial rows with a nil error.
var errMidStream = errors.New("connection reset by peer")

// rowsThatFailMidStream returns a sqlmock result that yields len(values)
// scannable rows and then reports err, the way database/sql iteration behaves
// when streaming fails after some rows have been consumed.
func rowsThatFailMidStream(columns []string, values [][]driver.Value, err error) *sqlmock.Rows {
	rows := sqlmock.NewRows(columns)
	for _, row := range values {
		rows = rows.AddRow(row...)
	}
	// The failing row index is the position where Next() stops early.
	return rows.RowError(len(values)-1, err)
}

func TestGetBackendsReportsMidStreamFailure(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()

	client := &postgreSQLClient{client: db, closeFn: func() error { return nil }}

	rows := rowsThatFailMidStream([]string{"datname", "count"},
		[][]driver.Value{{"db1", int64(3)}, {"db2", int64(1)}, {"db3", int64(2)}}, errMidStream)
	mock.ExpectQuery("SELECT datname, count(*) as count from pg_stat_activity GROUP BY datname;").WillReturnRows(rows)

	_, err = client.getBackends(t.Context(), nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMidStream)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetDatabaseStatsReportsMidStreamFailure(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()

	client := &postgreSQLClient{client: db, closeFn: func() error { return nil }}

	columns := []string{"datname", "xact_commit", "xact_rollback", "deadlocks", "temp_files", "temp_bytes", "tup_updated", "tup_returned", "tup_fetched", "tup_inserted", "tup_deleted", "blks_hit", "blks_read"}
	row := []driver.Value{"db1", int64(1), int64(2), int64(3), int64(4), int64(5), int64(6), int64(7), int64(8), int64(9), int64(10), int64(11), int64(12)}
	rows := rowsThatFailMidStream(columns, [][]driver.Value{row, row}, errMidStream)
	mock.ExpectQuery("SELECT datname, xact_commit, xact_rollback, deadlocks, temp_files, temp_bytes, tup_updated, tup_returned, tup_fetched, tup_inserted, tup_deleted, blks_hit, blks_read FROM pg_stat_database;").WillReturnRows(rows)

	_, err = client.getDatabaseStats(t.Context(), nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMidStream)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetDatabaseSizeReportsMidStreamFailure(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()

	client := &postgreSQLClient{client: db, closeFn: func() error { return nil }}

	rows := rowsThatFailMidStream([]string{"datname", "size"},
		[][]driver.Value{{"db1", int64(100)}, {"db2", int64(200)}}, errMidStream)
	mock.ExpectQuery("SELECT datname, pg_database_size(datname) FROM pg_catalog.pg_database WHERE datistemplate = false;").WillReturnRows(rows)

	_, err = client.getDatabaseSize(t.Context(), nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMidStream)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestListDatabasesReportsMidStreamFailure(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()

	client := &postgreSQLClient{client: db, closeFn: func() error { return nil }}

	rows := rowsThatFailMidStream([]string{"datname"},
		[][]driver.Value{{"db1"}, {"db2"}}, errMidStream)
	mock.ExpectQuery("SELECT datname FROM pg_database\nWHERE datistemplate = false;").WillReturnRows(rows)

	_, err = client.listDatabases(t.Context())
	require.Error(t, err)
	assert.ErrorIs(t, err, errMidStream)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetIndexStatsReportsMidStreamFailure(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()

	client := &postgreSQLClient{client: db, closeFn: func() error { return nil }}

	rows := rowsThatFailMidStream([]string{"schemaname", "relname", "indexrelname", "index_size", "idx_scan"},
		[][]driver.Value{{"public", "users", "users_pkey", int64(1024), int64(5)}, {"public", "users", "users_email_idx", int64(2048), int64(2)}}, errMidStream)
	mock.ExpectQuery("SELECT schemaname, relname, indexrelname,\n\tpg_relation_size(indexrelid) AS index_size,\n\tidx_scan\n\tFROM pg_stat_user_indexes s\n\tLEFT JOIN (\n\t\tSELECT DISTINCT relation\n\t\tFROM pg_locks\n\t\tWHERE locktype = 'relation'\n\t\t  AND mode = 'AccessExclusiveLock'\n\t\t  AND granted = true\n\t) l ON s.indexrelid = l.relation\n\tWHERE l.relation IS NULL;").WillReturnRows(rows)

	_, err = client.getIndexStats(t.Context(), "otel")
	require.Error(t, err)
	assert.ErrorIs(t, err, errMidStream)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetReplicationStatsReportsMidStreamFailure(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()

	client := &postgreSQLClient{client: db, closeFn: func() error { return nil }}

	rows := rowsThatFailMidStream([]string{"client_addr", "replication_bytes_pending", "write_lag_fractional", "flush_lag_fractional", "replay_lag_fractional"},
		[][]driver.Value{{"10.0.0.1", int64(1024), 1.5, 2.5, 3.5}, {"10.0.0.2", int64(2048), 4.5, 5.5, 6.5}}, errMidStream)
	mock.ExpectQuery("SELECT\n\tcoalesce(cast(client_addr as varchar), 'unix') AS client_addr,\n\tcoalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn), -1) AS replication_bytes_pending,\n\textract('epoch' from coalesce(write_lag, '-1 seconds'))::decimal AS write_lag_fractional,\n\textract('epoch' from coalesce(flush_lag, '-1 seconds'))::decimal AS flush_lag_fractional,\n\textract('epoch' from coalesce(replay_lag, '-1 seconds'))::decimal AS replay_lag_fractional\n\tFROM pg_stat_replication;\n\t").WillReturnRows(rows)

	_, err = client.getReplicationStats(t.Context())
	require.Error(t, err)
	assert.ErrorIs(t, err, errMidStream)
	require.NoError(t, mock.ExpectationsWereMet())
}
