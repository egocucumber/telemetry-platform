package query

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	queryv1 "github.com/egocucumber/telemetry-platform/gen/go/query/v1"
	"github.com/egocucumber/telemetry-platform/internal/redisx"
)

type GRPC struct {
	queryv1.UnimplementedQueryServiceServer
	svc *Service
}

func NewGRPC(svc *Service) *GRPC { return &GRPC{svc: svc} }

func (g *GRPC) GetLatest(ctx context.Context, req *queryv1.GetLatestRequest) (*queryv1.GetLatestResponse, error) {
	if req.GetDeviceId() == "" {
		return nil, status.Error(codes.InvalidArgument, "device_id is required")
	}
	latest, err := redisx.GetLatest(ctx, g.svc.rdb, req.GetDeviceId())
	if err != nil {
		return nil, status.Error(codes.Unavailable, "read model unavailable")
	}
	if len(latest) == 0 {
		return nil, status.Error(codes.NotFound, "no data for device")
	}
	resp := &queryv1.GetLatestResponse{DeviceId: req.GetDeviceId()}
	for metric, lv := range latest {
		resp.Values = append(resp.Values, &queryv1.MetricValue{Metric: metric, Value: lv.Value, Ts: timestamppb.New(lv.TS)})
	}
	return resp, nil
}

func (g *GRPC) GetHistory(ctx context.Context, req *queryv1.GetHistoryRequest) (*queryv1.GetHistoryResponse, error) {
	if req.GetDeviceId() == "" || req.GetMetric() == "" {
		return nil, status.Error(codes.InvalidArgument, "device_id and metric are required")
	}
	to := time.Now()
	from := to.Add(-time.Hour)
	if req.GetFrom() != nil {
		from = req.GetFrom().AsTime()
	}
	if req.GetTo() != nil {
		to = req.GetTo().AsTime()
	}
	step := time.Duration(req.GetStepSeconds()) * time.Second
	points, err := g.svc.repo.History(ctx, req.GetDeviceId(), req.GetMetric(), from, to, step)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, status.Error(codes.DeadlineExceeded, "query timeout")
		}
		return nil, status.Error(codes.Internal, "history query failed")
	}
	resp := &queryv1.GetHistoryResponse{}
	for _, p := range points {
		resp.Points = append(resp.Points, &queryv1.HistoryPoint{
			Bucket: timestamppb.New(p.Bucket), Avg: p.Avg, Min: p.Min, Max: p.Max, Count: p.Count,
		})
	}
	return resp, nil
}
