package collector

import (
	"context"
	"log/slog"
	"runtime/debug"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Metadata keys inspected for a correlation identifier, in priority order.
var traceHeaders = []string{"x-trace-id", "x-request-id", "traceparent"}

// traceIDFromContext extracts a correlation id from the incoming metadata so a
// batch can be followed from the browser to ClickHouse.
func traceIDFromContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	for _, header := range traceHeaders {
		values := md.Get(header)
		if len(values) == 0 || values[0] == "" {
			continue
		}
		if header == "traceparent" {
			// W3C format: version-traceid-spanid-flags.
			if parts := strings.Split(values[0], "-"); len(parts) >= 2 {
				return parts[1]
			}
			continue
		}
		return truncate(values[0], maxIDLen)
	}
	return ""
}

// RecoveryInterceptor turns a panicking handler into an INTERNAL error instead of
// tearing the process down. Losing one batch is better than losing all of them.
func RecoveryInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				log.Error("handler panic",
					slog.String("method", info.FullMethod),
					slog.Any("panic", r),
					slog.String("stack", string(debug.Stack())),
				)
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(ctx, req)
	}
}

// LoggingInterceptor logs failures at warn level and successes at debug level,
// keeping the hot path quiet.
func LoggingInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		attrs := []any{
			slog.String("method", info.FullMethod),
			slog.Duration("took", time.Since(start)),
			slog.String("code", status.Code(err).String()),
		}
		if err != nil {
			log.Warn("rpc failed", append(attrs, slog.Any("error", err))...)
			return resp, err
		}
		log.Debug("rpc served", attrs...)
		return resp, nil
	}
}
