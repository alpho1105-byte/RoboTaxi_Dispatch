package fare

import (
	"time"

	"robotaxi-dispatch/internal/domain"
	"robotaxi-dispatch/internal/geo"
)

// Estimator 估算車資。正式系統的估價需要離線資料管線與即時供需資料，
// 這裡只用「起跳 + 里程」，並以直線距離乘上繞路係數估算實際路程。
type Estimator struct {
	BaseCents    int64
	PerKmCents   int64
	MinimumCents int64
	DetourFactor float64
	TTL          time.Duration
}

func DefaultEstimator() Estimator {
	return Estimator{BaseCents: 300, PerKmCents: 150, MinimumCents: 700, DetourFactor: 1.3, TTL: 5 * time.Minute}
}

func (e Estimator) Estimate(pickup, destination domain.Point) (distanceM int, amountCents int64) {
	distanceM = int(geo.DistanceM(pickup, destination) * e.DetourFactor)
	amountCents = max(e.MinimumCents, e.BaseCents+e.PerKmCents*int64(distanceM)/1000)
	return distanceM, amountCents
}
