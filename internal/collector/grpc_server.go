package collector

import (
	"log/slog"
	"time"

	"github.com/alfacrab/nark/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	narkv1 "github.com/alfacrab/nark/gen/go/nark/v1"
)

func NewGRPCServer(cfg config.Collector, service *Service, log *slog.Logger) *grpc.Server {
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(cfg.MaxRecvMsgBytes),
		grpc.ChainUnaryInterceptor(
			RecoveryInterceptor(log),
			LoggingInterceptor(log),
		),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 5 * time.Minute,
			Time:              2 * time.Minute,
			Timeout:           20 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             30 * time.Second,
			PermitWithoutStream: true,
		}),
	)

	narkv1.RegisterTrackIngestServiceServer(srv, service)

	// healthServer := health.NewServer()
	// healthServer.SetServingStatus("nark.v1.TrackIngest", healthpb.HealthCheckResponse_SERVING)
	// healthpb.RegisterHealthServer(srv, healthServer)

	if cfg.Runtime.Env == "local" {
		reflection.Register(srv)
	}
	return srv
}
