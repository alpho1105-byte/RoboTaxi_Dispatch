package geo

import (
	"math"

	"robotaxi-dispatch/internal/domain"
)

const earthRadiusM = 6_371_008.8

// DistanceM 回傳兩點之間的大圓距離（公尺）。
func DistanceM(a, b domain.Point) float64 {
	lat1, lat2 := a.Lat*math.Pi/180, b.Lat*math.Pi/180
	dLat := lat2 - lat1
	dLng := (b.Lng - a.Lng) * math.Pi / 180
	h := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLng/2)*math.Sin(dLng/2)
	return 2 * earthRadiusM * math.Asin(math.Min(1, math.Sqrt(h)))
}

// MoveToward 從 from 朝 to 前進 meters 公尺，回傳新位置與是否已抵達。
// 短距離內把經緯度當平面處理就夠精確。
func MoveToward(from, to domain.Point, meters float64) (domain.Point, bool) {
	d := DistanceM(from, to)
	if d <= meters || d == 0 {
		return to, true
	}
	f := meters / d
	return domain.Point{Lat: from.Lat + (to.Lat-from.Lat)*f, Lng: from.Lng + (to.Lng-from.Lng)*f}, false
}

// Offset 回傳從 p 往北 north 公尺、往東 east 公尺的位置。
func Offset(p domain.Point, north, east float64) domain.Point {
	dLat := north / earthRadiusM * 180 / math.Pi
	dLng := east / (earthRadiusM * math.Cos(p.Lat*math.Pi/180)) * 180 / math.Pi
	return domain.Point{Lat: p.Lat + dLat, Lng: p.Lng + dLng}
}

// MetersPerDegreeLat 是緯度一度對應的公尺數。
const MetersPerDegreeLat = earthRadiusM * math.Pi / 180
