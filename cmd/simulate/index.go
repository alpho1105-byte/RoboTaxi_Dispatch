package main

import (
	"fmt"
	"math"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"robotaxi-dispatch/internal/domain"
	"robotaxi-dispatch/internal/geo"
	"robotaxi-dispatch/internal/location"
)

var center = domain.Point{Lat: 25.0330, Lng: 121.5654}

// benchmarkIndex 讓所有 CPU 同時更新 10 萬台車的位置，再量測找附近空車的查詢延遲。
func benchmarkIndex(seed uint64) {
	const vehicles = 100_000
	const radiusM = 15_000
	idx := location.NewIndex(0.005)
	rng := rand.New(rand.NewPCG(seed, 1))
	positions := make([]domain.Point, vehicles)
	ids := make([]string, vehicles)
	for i := range positions {
		r, theta := radiusM*math.Sqrt(rng.Float64()), 2*math.Pi*rng.Float64()
		positions[i] = geo.Offset(center, r*math.Cos(theta), r*math.Sin(theta))
		ids[i] = fmt.Sprintf("av-%06d", i)
		idx.Update(ids[i], positions[i], 0.2+0.8*rng.Float64(), rng.IntN(3) == 0, time.Now())
	}

	workers := runtime.GOMAXPROCS(0)
	var total atomic.Int64
	var stop atomic.Bool
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(seed, uint64(w)+10))
			n := int64(0)
			// 每個 goroutine 負責自己那一份車，模擬各自獨立的車輛連線。
			for i := w; !stop.Load(); i += workers {
				if i >= vehicles {
					i = w
				}
				positions[i] = geo.Offset(positions[i], r.Float64()*40-20, r.Float64()*40-20)
				idx.Update(ids[i], positions[i], 0.5, false, time.Now())
				n++
			}
			total.Add(n)
		}()
	}
	const duration = 2 * time.Second
	time.Sleep(duration)
	stop.Store(true)
	wg.Wait()

	// 單次查詢比 Windows 計時器的解析度還短，所以量一整批的總時間再平均。
	const queries = 10_000
	points := make([]domain.Point, queries)
	for i := range points {
		r, theta := radiusM*math.Sqrt(rng.Float64()), 2*math.Pi*rng.Float64()
		points[i] = geo.Offset(center, r*math.Cos(theta), r*math.Sin(theta))
	}
	start := time.Now()
	for _, p := range points {
		idx.Nearby(p, 5, 5_000, 0.2, nil)
	}
	avg := time.Since(start) / queries

	fmt.Printf("A. location index: %d vehicles within %d km, %d goroutines\n", vehicles, radiusM/1000, workers)
	fmt.Printf("  updates   %.1f million/s\n", float64(total.Load())/duration.Seconds()/1e6)
	fmt.Printf("  nearby    5 closest available within 5 km: avg %s per query over %d queries\n\n", ms(avg), queries)
}
