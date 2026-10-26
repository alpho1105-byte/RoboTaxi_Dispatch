package clock

import (
	"context"
	"time"
)

// Sleep 等待 d 或直到 ctx 結束；正常睡完回傳 true。
func Sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
