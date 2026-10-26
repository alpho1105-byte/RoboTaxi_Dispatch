// simulate 分兩部分：
//
//	A. 位置索引壓測：大量車輛同時回報位置時的更新吞吐量，以及找附近空車的查詢延遲。
//	B. 演唱會散場：短時間內湧入大量叫車，車輛會拒絕、逾時、晚回覆，甚至在載客時誤接新單，
//	   途中再讓一個 matching instance 卡死。驗證每筆請求都在一分鐘內有結果、不會遺失，
//	   而且沒有一趟行程派給兩台車、也沒有一台車同時接兩趟。
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"
)

func main() {
	skipIndex := flag.Bool("skip-index", false, "skip part A (location index benchmark)")
	seed := flag.Uint64("seed", 42, "random seed")
	verbose := flag.Bool("v", false, "print component logs")
	flag.Parse()

	level := slog.LevelError
	if *verbose {
		level = slog.LevelInfo
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	if !*skipIndex {
		benchmarkIndex(*seed)
	}
	if err := flashCrowd(*seed, logger); err != nil {
		fmt.Fprintln(os.Stderr, "simulation failed:", err)
		os.Exit(1)
	}
}

var failures int

func check(desc string, ok bool) {
	mark := "ok  "
	if !ok {
		mark = "FAIL"
		failures++
	}
	fmt.Printf("  [%s] %s\n", mark, desc)
}

func ms(d time.Duration) string {
	switch {
	case d >= time.Second:
		return fmt.Sprintf("%.2fs", d.Seconds())
	case d >= time.Millisecond:
		return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000)
	default:
		return fmt.Sprintf("%dµs", d.Microseconds())
	}
}
