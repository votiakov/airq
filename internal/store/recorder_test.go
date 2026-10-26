package store

import (
	"bytes"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/votiakov/airq/internal/upstream"
)

// The queue must not pin full vendor responses while the database is slow or down.
func TestRecordBoundsQueuedBodyMemory(t *testing.T) {
	db, err := Open(Options{URL: "postgres://u:p@127.0.0.1:1/db", MaxOpenConns: 1}) // never connected
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	reg := prometheus.NewRegistry()
	rec := NewRecorder(db, NewMetrics(reg, db), RecorderOptions{
		Buffer: 4, BatchSize: 4, FlushInterval: time.Hour, MaxBodyBytes: 16, Registerer: reg,
	})

	big := bytes.Repeat([]byte("x"), 1<<20)
	small := make([]byte, 8, 4096) // io.ReadAll leaves spare capacity like this
	copy(small, `{"a":1}`)
	rec.Record(upstream.Exchange{Body: big})
	rec.Record(upstream.Exchange{Body: small})

	q := <-rec.queue
	if q.Body != nil || q.bodyBytes != len(big) {
		t.Errorf("oversized body must be dropped before queueing (len %d), size kept as %d", len(q.Body), q.bodyBytes)
	}
	if row := rec.row(q); row[9] != len(big) || row[10] != nil || row[11] != true {
		t.Errorf("oversized row = bytes %v body %v truncated %v", row[9], row[10], row[11])
	}
	q = <-rec.queue
	if cap(q.Body) != len(small) || !bytes.Equal(q.Body, small) {
		t.Errorf("small body must be copied exactly: len %d cap %d", len(q.Body), cap(q.Body))
	}
}
