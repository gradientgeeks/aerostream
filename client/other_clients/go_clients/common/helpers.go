package common

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"
)

// ANSI color codes for rich CLI reporting
const (
	ColorReset  = "\033[0m"
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorBlue   = "\033[34m"
	ColorPurple = "\033[35m"
	ColorCyan   = "\033[36m"
	ColorWhite  = "\033[37m"
	ColorBold   = "\033[1m"
)

// ComputeSHA256 returns the hexadecimal SHA-256 hash of a byte slice.
func ComputeSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// LatencyStats holds percentile latencies.
type LatencyStats struct {
	Count int
	Min   time.Duration
	Max   time.Duration
	Mean  time.Duration
	P50   time.Duration
	P90   time.Duration
	P95   time.Duration
	P99   time.Duration
}

// CalculateLatencies computes summary and percentile statistics from a list of durations.
func CalculateLatencies(durations []time.Duration) LatencyStats {
	if len(durations) == 0 {
		return LatencyStats{}
	}
	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var total time.Duration
	for _, d := range sorted {
		total += d
	}

	pIndex := func(p float64) time.Duration {
		idx := int(float64(len(sorted)-1) * p)
		return sorted[idx]
	}

	return LatencyStats{
		Count: len(sorted),
		Min:   sorted[0],
		Max:   sorted[len(sorted)-1],
		Mean:  total / time.Duration(len(sorted)),
		P50:   pIndex(0.50),
		P90:   pIndex(0.90),
		P95:   pIndex(0.95),
		P99:   pIndex(0.99),
	}
}

// CreateTopicViaREST calls AeroStream's Controller REST API to register a new topic with the given partition count.
func CreateTopicViaREST(controllerAddr, topic string, partitions int) error {
	url := fmt.Sprintf("%s/api/topics", controllerAddr)
	payload := map[string]interface{}{
		"name":               topic,
		"partitions":         partitions,
		"replication_factor": 1,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("POST %s failed: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("failed to create topic %s: status %d", topic, resp.StatusCode)
	}
	return nil
}
