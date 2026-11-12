package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

var (
	testTraceID = trace.TraceID{1, 2, 3}
	testSpanID  = trace.SpanID{4, 5, 6}
)

// newTestLogger returns a logger that writes JSON into the returned buffer.
func newTestLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(WithTrace(slog.NewJSONHandler(&buf, nil))), &buf
}

// contextWithSpan puts a span context with known ids into ctx, the way
// otelhttp does for a real request.
func contextWithSpan(ctx context.Context) context.Context {
	return trace.ContextWithSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: testTraceID,
		SpanID:  testSpanID,
	}))
}

// lastRecord decodes the last JSON line written to buf.
func lastRecord(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))

	var rec map[string]any
	require.NoError(t, json.Unmarshal(lines[len(lines)-1], &rec))

	return rec
}

func TestWithTraceAddsIDs(t *testing.T) {
	logger, buf := newTestLogger()

	logger.InfoContext(contextWithSpan(t.Context()), "hello")

	rec := lastRecord(t, buf)
	require.Equal(t, testTraceID.String(), rec["trace_id"])
	require.Equal(t, testSpanID.String(), rec["span_id"])
}

func TestWithTraceSurvivesWith(t *testing.T) {
	logger, buf := newTestLogger()

	// every service builds its logger with With, which goes through WithAttrs:
	// if WithAttrs dropped the wrapper, no record would ever get a trace_id
	logger.With("component", "api").InfoContext(contextWithSpan(t.Context()), "hello")

	rec := lastRecord(t, buf)
	require.Equal(t, "api", rec["component"])
	require.Equal(t, testTraceID.String(), rec["trace_id"])
}

func TestWithTraceSurvivesWithGroup(t *testing.T) {
	logger, buf := newTestLogger()

	logger.WithGroup("request").InfoContext(contextWithSpan(t.Context()), "hello", "status", 200)

	rec := lastRecord(t, buf)
	group, ok := rec["request"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, testTraceID.String(), group["trace_id"])
}

func TestWithTraceWithoutSpan(t *testing.T) {
	logger, buf := newTestLogger()

	// a record outside of any request, like "config loaded" at start
	logger.InfoContext(t.Context(), "hello")

	rec := lastRecord(t, buf)
	require.NotContains(t, rec, "trace_id")
	require.NotContains(t, rec, "span_id")
}
