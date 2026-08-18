package observability

import (
	"io"
	"log/slog"
	"strings"

	"github.com/alfacrab/nark/internal/config"
)

// NewLogger builds the process logger. Every record carries the service, env and instance labels so logs can be correlated with metrics.
func NewLogger(w io.Writer, rt config.Runtime) *slog.Logger {
	opts := &slog.HandlerOptions{Level: parseLevel(rt.LogLevel)}

	var handler slog.Handler
	if strings.EqualFold(rt.LogFormat, "text") {
		handler = slog.NewTextHandler(w, opts)
	} else {
		handler = slog.NewJSONHandler(w, opts)
	}

	return slog.New(handler).With(
		slog.String("service", rt.Service),
		slog.String("env", rt.Env),
		slog.String("instance", rt.Instance),
	)
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
