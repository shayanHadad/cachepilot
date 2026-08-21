package logger

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/shayanHadad/cachepilot/internal/types"
)

// flushInterval controls how often buffered log data is flushed to
// disk even if the logger stays busy and Close() hasn't been called yet.
const flushInterval = 1 * time.Second

// Logger asynchronously writes LogEntry records to a JSONL file.
type Logger struct {
	entries chan types.LogEntry
	file    *os.File
	writer  *bufio.Writer
	slogger *slog.Logger
	done    chan struct{}

	dropped     atomic.Int64
	writeErrors atomic.Int64
}

// NewLogger opens (creating if needed) the JSONL file at path in
// append mode and starts the background writer goroutine.
func NewLogger(path string, bufferSize int) (*Logger, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("logger: failed to create log directory: %w", err)
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("logger: failed to open %s: %w", path, err)
	}

	writer := bufio.NewWriter(file)
	handler := slog.NewJSONHandler(writer, &slog.HandlerOptions{
		ReplaceAttr: dropBuiltinAttrs,
	})

	l := &Logger{
		entries: make(chan types.LogEntry, bufferSize),
		file:    file,
		writer:  writer,
		slogger: slog.New(handler),
		done:    make(chan struct{}),
	}

	go l.run()
	return l, nil
}

// dropBuiltinAttrs removes slog's default "time", "level", and "msg"
// keys from every log line, so each JSONL line contains exactly the
// LogEntry fields the data pipeline expects.
func dropBuiltinAttrs(groups []string, a slog.Attr) slog.Attr {
	switch a.Key {
	case slog.TimeKey, slog.LevelKey, slog.MessageKey:
		return slog.Attr{}
	}
	return a
}

// Log enqueues entry to be written asynchronously. It never blocks:
// if the internal buffer is full, entry is dropped and the dropped
// counter is incremented instead of waiting for room.
func (l *Logger) Log(entry types.LogEntry) {
	select {
	case l.entries <- entry:
	default:
		l.dropped.Add(1)
	}
}

// run is the background writer goroutine: it reads entries from the
// channel and periodically flushes to disk, until the channel is
// closed and fully drained.
func (l *Logger) run() {
	defer close(l.done)

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case entry, ok := <-l.entries:
			if !ok {
				// Channel closed by Close() and fully drained —
				// nothing left to read. Close() does its own final
				// Flush() right after this goroutine exits, so we
				// don't need to flush here too.
				return
			}
			l.writeEntry(entry)

		case <-ticker.C:
			if err := l.writer.Flush(); err != nil {
				fmt.Fprintf(os.Stderr, "logger: periodic flush failed: %v\n", err)
			}
		}
	}
}

// writeEntry builds a slog.Record from entry and hands it directly
// to the handler, so a write failure can be observed and counted.
func (l *Logger) writeEntry(entry types.LogEntry) {
	record := slog.NewRecord(time.Now(), slog.LevelInfo, "", 0)
	record.AddAttrs(
		slog.Int64("timestamp", entry.Timestamp),
		slog.String("key", entry.Key),
		slog.String("cache_status", string(entry.CacheStatus)),
		slog.Float64("latency_ms", entry.LatencyMs),
		slog.Int("response_size", entry.ResponseSize),
		slog.String("query_type", entry.QueryType),
	)

	if err := l.slogger.Handler().Handle(context.Background(), record); err != nil {
		l.writeErrors.Add(1)
		fmt.Fprintf(os.Stderr, "logger: failed to write entry: %v\n", err)
	}
}

// Dropped returns the number of log entries dropped because the
// internal buffer was full.
func (l *Logger) Dropped() int64 {
	return l.dropped.Load()
}

// WriteErrors returns the number of entries that were dequeued but
// failed to write to disk.
func (l *Logger) WriteErrors() int64 {
	return l.writeErrors.Load()
}

// Close stops accepting new entries, waits for the writer goroutine
// to drain the remaining buffered entries, flushes any buffered
// bytes to disk, and closes the underlying file.
func (l *Logger) Close() error {
	close(l.entries)
	<-l.done // wait for run() to finish draining the channel

	if err := l.writer.Flush(); err != nil {
		return fmt.Errorf("logger: failed to flush: %w", err)
	}
	if err := l.file.Close(); err != nil {
		return fmt.Errorf("logger: failed to close file: %w", err)
	}
	return nil
}
