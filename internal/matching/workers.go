package matching

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"robotaxi-dispatch/internal/domain"
	"robotaxi-dispatch/internal/queue"
)

// prefetch 是每個 instance 一次從 queue 取出的請求數，減少和 queue 的往返。
// 代價是 instance 當掉時，手上這一整批都要等租約到期才會被重新投遞。
const prefetch = 16

// Workers 是多個無狀態的 Matching Service instance。每個 instance 都會：
// 從 queue 取行程請求、處理車輛回覆、定期處理逾時。任何一個 instance 都能處理任何一趟行程。
type Workers struct {
	m         *Matcher
	requests  *queue.Leased[domain.RideRequest]
	decisions <-chan domain.DispatchDecision
	n         int

	hung     []atomic.Bool
	stranded atomic.Int64
}

func NewWorkers(m *Matcher, requests *queue.Leased[domain.RideRequest], decisions <-chan domain.DispatchDecision, n int) *Workers {
	return &Workers{m: m, requests: requests, decisions: decisions, n: n, hung: make([]atomic.Bool, n)}
}

func (w *Workers) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := range w.n {
		for _, loop := range []func(context.Context, int){w.requestLoop, w.decisionLoop, w.timeoutLoop} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				loop(ctx, i)
			}()
		}
	}
	wg.Wait()
}

// 行程請求在配對開始、狀態寫入 Store 之後才 Ack。instance 在那之前掛掉，
// 租約到期後請求會被其他 instance 重新取得，不會遺失。
func (w *Workers) requestLoop(ctx context.Context, i int) {
	for {
		batch, err := w.requests.Receive(ctx, prefetch)
		if err != nil {
			return
		}
		for k, msg := range batch {
			if w.hung[i].Load() {
				// 卡住的 instance 不再處理任何東西，手上還沒 Ack 的請求只能等租約到期。
				w.stranded.Add(int64(len(batch) - k))
				<-ctx.Done()
				return
			}
			w.m.Start(ctx, msg.Body)
			w.requests.Ack(msg.ID)
		}
	}
}

func (w *Workers) decisionLoop(ctx context.Context, i int) {
	for {
		select {
		case <-ctx.Done():
			return
		case d := <-w.decisions:
			if w.hung[i].Load() {
				// 卡住時拿到的回覆就此遺失；那台車的詢問會在逾時後由其他 instance 接手。
				<-ctx.Done()
				return
			}
			w.m.OnDecision(ctx, d)
		}
	}
}

func (w *Workers) timeoutLoop(ctx context.Context, i int) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if w.hung[i].Load() {
				return
			}
			w.m.OnTimeouts(ctx)
		}
	}
}

// Hang 模擬第 i 個 instance 卡死（例如死結或長時間 GC）：它停在處理到一半的地方，不再做任何事。
func (w *Workers) Hang(i int) {
	w.hung[i].Store(true)
}

// Stranded 回傳卡住的 instance 手上沒處理完的請求數。
func (w *Workers) Stranded() int64 {
	return w.stranded.Load()
}
