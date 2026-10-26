package app

import (
	"context"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"robotaxi-dispatch/internal/domain"
	"robotaxi-dispatch/internal/fare"
	"robotaxi-dispatch/internal/gateway"
	"robotaxi-dispatch/internal/location"
	"robotaxi-dispatch/internal/matching"
	"robotaxi-dispatch/internal/metrics"
	"robotaxi-dispatch/internal/queue"
	"robotaxi-dispatch/internal/ride"
)

type Options struct {
	DataDir  string
	Matching matching.Config
	Workers  int
	// Visibility 是行程請求的租約時間，matching instance 在這段時間內沒有 Ack 就會被重新投遞。
	Visibility time.Duration
	CellDeg    float64
	Logger     *slog.Logger
}

func DefaultOptions(dataDir string) Options {
	return Options{
		DataDir: dataDir, Matching: matching.DefaultConfig(), Workers: 4,
		Visibility: 5 * time.Second, CellDeg: 0.005, Logger: slog.Default(),
	}
}

// App 是 composition root。叫車請求的流程：Ride Service 寫入 Ride DB 並放進 queue →
// Matching workers 取出請求、查位置索引、透過 AV Gateway 派車 → 車輛接受後以 RPC 回寫 Ride DB。
type App struct {
	Rides        *ride.Store
	RideService  *ride.Service
	Locations    *location.Index
	Requests     *queue.Leased[domain.RideRequest]
	Gateway      *gateway.Gateway
	States       *matching.Store
	Matcher      *matching.Matcher
	Workers      *matching.Workers
	MatchLatency *metrics.Latency
}

func New(opts Options) (*App, error) {
	store, err := ride.Open(filepath.Join(opts.DataDir, "rides.db"))
	if err != nil {
		return nil, err
	}
	a := &App{
		Rides:        store,
		Locations:    location.NewIndex(opts.CellDeg),
		Requests:     queue.NewLeased[domain.RideRequest](opts.Visibility),
		States:       matching.NewStore(),
		MatchLatency: &metrics.Latency{},
	}
	a.RideService = ride.NewService(store, fare.DefaultEstimator(), a.Requests)
	a.Gateway = gateway.New(a.Locations, a.RideService, 4096)
	a.Matcher = matching.NewMatcher(opts.Matching, a.States, a.Locations, a.RideService, a.Gateway, a.MatchLatency,
		opts.Logger.With("component", "matching"))
	a.Workers = matching.NewWorkers(a.Matcher, a.Requests, a.Gateway.Decisions(), opts.Workers)
	return a, nil
}

func (a *App) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		a.Requests.Run(ctx, 200*time.Millisecond)
	}()
	go func() {
		defer wg.Done()
		a.Workers.Run(ctx)
	}()
	wg.Wait()
}

func (a *App) Close() error {
	return a.Rides.Close()
}
