package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"robotaxi-dispatch/internal/app"
	"robotaxi-dispatch/internal/domain"
	"robotaxi-dispatch/internal/fleet"
	"robotaxi-dispatch/internal/geo"
	"robotaxi-dispatch/internal/matching"
)

// 時間壓縮：車速 300 m/s，讓接送一趟只需要幾秒到十幾秒，整個尖峰在兩分鐘內跑完。
// 配對的期限仍然是真實的 60 秒。
const (
	fleetSize      = 5_000
	fleetRadiusM   = 6_000
	crowdRequests  = 12_000
	crowdWindow    = 10 * time.Second
	crowdRadiusM   = 600
	backgroundRide = 1_500
	backgroundSpan = 30 * time.Second
	// requirement 是需求本身的一分鐘；matching 內部用 55 秒的期限，留出餘裕。
	requirement = time.Minute
)

type request struct {
	pickup, destination domain.Point
	at                  time.Duration
}

func flashCrowd(seed uint64, logger *slog.Logger) error {
	dir, err := os.MkdirTemp("", "robotaxi-sim-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	opts := app.DefaultOptions(dir)
	opts.Logger = logger
	opts.Workers = 8
	opts.Visibility = 3 * time.Second
	opts.Matching = matching.Config{
		Candidates: 5, SearchRadiusM: 6_000, MinBattery: 0.2, OfferTimeout: 2 * time.Second,
		Deadline: matching.DefaultConfig().Deadline, RetryEvery: 500 * time.Millisecond, ReservationTTL: 8 * time.Second,
	}
	a, err := app.New(opts)
	if err != nil {
		return err
	}
	defer a.Close()

	f := fleet.New(a.Gateway, fleet.DefaultBehavior())
	f.Spawn(fleetSize, center, fleetRadiusM, seed)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for _, run := range []func(context.Context){a.Run, f.Run} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			run(ctx)
		}()
	}
	defer func() {
		cancel()
		wg.Wait()
	}()
	time.Sleep(500 * time.Millisecond)

	reqs := plan(seed)
	fmt.Printf("B. flash crowd: %d vehicles, %d requests within %dm of the venue in %s plus %d citywide over %s\n",
		fleetSize, crowdRequests, crowdRadiusM, crowdWindow, backgroundRide, backgroundSpan)
	fmt.Println("   vehicles reject 3%, ignore 3% (half reply late); 5% misreport themselves as free and accept new rides while busy half the time")

	start := time.Now()
	hangAt := 4 * time.Second
	go func() {
		time.Sleep(hangAt)
		a.Workers.Hang(0)
	}()
	retried, sameRide, err := send(ctx, a, reqs, start)
	if err != nil {
		return err
	}
	lastRequest := time.Now()
	fmt.Printf("  sent      %d requests in %s (planned %s)\n", len(reqs), msStr(lastRequest.Sub(start)), msStr(reqs[len(reqs)-1].at))

	// 等每一趟都有結果：配對成功，或在期限到時宣告失敗。
	var resolvedAt time.Time
	if err := waitFor(lastRequest.Add(requirement+5*time.Second), func() bool {
		counts, err := a.Rides.CountByStatus(ctx)
		if err == nil && counts[domain.RideRequested] == 0 {
			resolvedAt = time.Now()
			return true
		}
		return false
	}); err != nil {
		counts, _ := a.Rides.CountByStatus(ctx)
		return fmt.Errorf("rides still waiting for a match: %v, matching states %v, matcher %+v, queue %+v",
			counts, a.States.Counts(), a.Matcher.Stats(), a.Requests.Stats())
	}
	// 再等配對成功的行程全部跑完。
	waitFor(time.Now().Add(90*time.Second), func() bool {
		counts, err := a.Rides.CountByStatus(ctx)
		return err == nil && counts[domain.RideAssigned] == 0 && counts[domain.RideInProgress] == 0
	})

	return report(ctx, a, f, len(reqs), retried, sameRide, resolvedAt.Sub(lastRequest), hangAt)
}

func plan(seed uint64) []request {
	rng := rand.New(rand.NewPCG(seed, 2))
	venue := geo.Offset(center, 1_000, 0)
	around := func(p domain.Point, radius float64) domain.Point {
		r, theta := radius*math.Sqrt(rng.Float64()), 2*math.Pi*rng.Float64()
		return geo.Offset(p, r*math.Cos(theta), r*math.Sin(theta))
	}
	destination := func(from domain.Point) domain.Point {
		d, theta := 1_000+7_000*rng.Float64(), 2*math.Pi*rng.Float64()
		return geo.Offset(from, d*math.Cos(theta), d*math.Sin(theta))
	}
	var reqs []request
	for i := range crowdRequests {
		p := around(venue, crowdRadiusM)
		reqs = append(reqs, request{pickup: p, destination: destination(p), at: crowdWindow * time.Duration(i) / crowdRequests})
	}
	for i := range backgroundRide {
		p := around(center, fleetRadiusM)
		reqs = append(reqs, request{pickup: p, destination: destination(p), at: backgroundSpan * time.Duration(i) / backgroundRide})
	}
	sortByTime(reqs)
	return reqs
}

func sortByTime(reqs []request) {
	for i := 1; i < len(reqs); i++ {
		for j := i; j > 0 && reqs[j].at < reqs[j-1].at; j-- {
			reqs[j], reqs[j-1] = reqs[j-1], reqs[j]
		}
	}
}

// send 依排定的時間送出叫車。約 1% 的乘客因為網路問題用同一張估價單重送一次。
func send(ctx context.Context, a *app.App, reqs []request, start time.Time) (retried, sameRide int64, err error) {
	var wg sync.WaitGroup
	var retries, same atomic.Int64
	var firstErr atomic.Value
	jobs := make(chan int, 1024)
	for w := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w), 3))
			for i := range jobs {
				r := reqs[i]
				if d := time.Until(start.Add(r.at)); d > 0 {
					time.Sleep(d)
				}
				rider := fmt.Sprintf("rider-%05d", i)
				fare, err := a.RideService.EstimateFare(ctx, rider, r.pickup, r.destination)
				if err != nil {
					firstErr.CompareAndSwap(nil, err)
					continue
				}
				ride, _, err := a.RideService.RequestRide(ctx, rider, fare.ID)
				if err != nil {
					firstErr.CompareAndSwap(nil, err)
					continue
				}
				if rng.Float64() < 0.01 {
					retries.Add(1)
					if again, created, err := a.RideService.RequestRide(ctx, rider, fare.ID); err == nil && !created && again.ID == ride.ID {
						same.Add(1)
					}
				}
			}
		}()
	}
	for i := range reqs {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	if v := firstErr.Load(); v != nil {
		return 0, 0, v.(error)
	}
	return retries.Load(), same.Load(), nil
}

func report(ctx context.Context, a *app.App, f *fleet.Fleet, total int, retried, sameRide int64, resolvedAfterLast, hangAt time.Duration) error {
	rides, err := a.Rides.All(ctx)
	if err != nil {
		return err
	}
	counts := make(map[domain.RideStatus]int)
	activePerAV := make(map[string]int)
	var late int
	var slowest time.Duration
	for _, r := range rides {
		counts[r.Status]++
		if r.Status == domain.RideAssigned || r.Status == domain.RideInProgress {
			activePerAV[r.AVID]++
		}
		took := r.ResolvedAt.Sub(r.RequestedAt)
		slowest = max(slowest, took)
		if r.ResolvedAt.IsZero() || took > requirement {
			late++
		}
	}
	served := 0
	doubleServed := 0
	for _, avs := range f.ServedBy() {
		served++
		if len(avs) > 1 {
			doubleServed++
		}
	}
	doubleActive := 0
	for _, n := range activePerAV {
		if n > 1 {
			doubleActive++
		}
	}

	ms := a.Matcher.Stats()
	qs := a.Requests.Stats()
	lat := a.MatchLatency.Summary()
	matched := counts[domain.RideCompleted] + counts[domain.RideAssigned] + counts[domain.RideInProgress]
	fmt.Printf("  result    %d rides: %d matched (%d trips completed), %d failed at the %s matching deadline\n",
		len(rides), matched, counts[domain.RideCompleted], counts[domain.RideNoAVAvailable], matching.DefaultConfig().Deadline)
	fmt.Printf("  latency   request -> matched p50=%s p95=%s p99=%s max=%s\n", msStr(lat.P50), msStr(lat.P95), msStr(lat.P99), msStr(lat.Max))
	fmt.Printf("  dispatch  %d offers: %d accepted, %d rejected, %d timed out, %d late accepts withdrawn, %d blocked by the unique index\n",
		ms.Offers, ms.Accepts, ms.Rejects, ms.Timeouts, ms.LateAccepts, ms.Conflicts)
	fmt.Printf("  queue     max depth %d; one instance hung at %s holding %d requests, %d redelivered after their leases expired\n",
		qs.MaxDepth, hangAt, a.Workers.Stranded(), qs.Redelivered)
	fmt.Printf("  location  %d position updates from the fleet\n\n", a.Locations.Updates())

	fmt.Println("verify")
	check(fmt.Sprintf("every request got a result: none still waiting %s after the last request", msStr(resolvedAfterLast)),
		counts[domain.RideRequested] == 0 && len(rides) == total)
	check(fmt.Sprintf("every ride was matched or failed within %s of its request (slowest %s)", requirement, msStr(slowest)), late == 0)
	check(fmt.Sprintf("no ride was driven by two vehicles (%d rides served)", served), doubleServed == 0)
	check("no vehicle was confirmed for a second ride while on a trip", f.Overlaps() == 0)
	check("ride DB never holds two active rides for one vehicle", doubleActive == 0)
	check(fmt.Sprintf("%d retried requests with the same fare returned the existing ride", retried), retried == sameRide)
	check(fmt.Sprintf("every matched ride finished its trip (%d completed)", counts[domain.RideCompleted]),
		counts[domain.RideAssigned] == 0 && counts[domain.RideInProgress] == 0 && int64(counts[domain.RideCompleted]) == f.Completed())
	if failures > 0 {
		return fmt.Errorf("%d checks failed", failures)
	}
	return nil
}

func msStr(d time.Duration) string {
	return ms(d)
}

func waitFor(deadline time.Time, cond func() bool) error {
	for !cond() {
		if time.Now().After(deadline) {
			return errors.New("timed out")
		}
		time.Sleep(200 * time.Millisecond)
	}
	return nil
}
