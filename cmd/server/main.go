package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"robotaxi-dispatch/internal/api"
	"robotaxi-dispatch/internal/app"
	"robotaxi-dispatch/internal/auth"
	"robotaxi-dispatch/internal/domain"
	"robotaxi-dispatch/internal/fleet"
)

func main() {
	addr := flag.String("addr", ":8094", "HTTP listen address")
	dataDir := flag.String("data", "data", "directory for the ride database")
	fleetSize := flag.Int("fleet", 300, "number of simulated vehicles connected to the gateway (0 to disable)")
	lat := flag.Float64("lat", 25.0330, "center latitude of the simulated fleet")
	lng := flag.Float64("lng", 121.5654, "center longitude of the simulated fleet")
	dev := flag.Bool("dev", true, "enable POST /dev/token")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := run(*addr, *dataDir, *fleetSize, domain.Point{Lat: *lat, Lng: *lng}, *dev, logger); err != nil {
		logger.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

func run(addr, dataDir string, fleetSize int, center domain.Point, dev bool, logger *slog.Logger) error {
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		if !dev {
			return errors.New("JWT_SECRET is required when -dev=false")
		}
		secret = "dev-only-secret"
		logger.Warn("JWT_SECRET not set, using an insecure development secret")
	}

	opts := app.DefaultOptions(dataDir)
	opts.Logger = logger
	a, err := app.New(opts)
	if err != nil {
		return err
	}
	defer a.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	start := func(fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn()
		}()
	}
	start(func() { a.Run(ctx) })
	if fleetSize > 0 {
		// 本機 demo 用：一批模擬車輛連上 AV Gateway，以正常速度行駛。
		beh := fleet.DefaultBehavior()
		beh.SpeedMps, beh.Tick = 15, time.Second
		f := fleet.New(a.Gateway, beh)
		f.Spawn(fleetSize, center, 4_000, uint64(time.Now().UnixNano()))
		start(func() { f.Run(ctx) })
	}

	srv := &http.Server{
		Addr:              addr,
		Handler:           api.NewRouter(api.Deps{Signer: auth.NewSigner([]byte(secret)), Rides: a.RideService, Locations: a.Locations, Dev: dev}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	logger.Info("robotaxi dispatch server listening", "addr", addr, "fleet", fleetSize)
	err = srv.ListenAndServe()
	stop()
	wg.Wait()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
