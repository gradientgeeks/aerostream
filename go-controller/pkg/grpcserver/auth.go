package grpcserver

import (
	"context"
	"crypto/subtle"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TokenAuthInterceptor returns a unary server interceptor that requires every
// request to carry a matching bearer token in the "authorization" metadata.
// An empty token disables authentication (interceptor passes everything through).
func TokenAuthInterceptor(token string) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		if token == "" {
			return handler(ctx, req)
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing request metadata")
		}
		vals := md.Get("authorization")
		if len(vals) == 0 {
			return nil, status.Error(codes.Unauthenticated, "missing authorization token")
		}

		provided := strings.TrimPrefix(vals[0], "Bearer ")
		// Constant-time comparison to avoid timing side channels.
		if subtle.ConstantTimeCompare([]byte(provided), []byte(token)) != 1 {
			return nil, status.Error(codes.Unauthenticated, "invalid authorization token")
		}

		return handler(ctx, req)
	}
}
