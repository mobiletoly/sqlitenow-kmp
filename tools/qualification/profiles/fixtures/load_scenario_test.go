// Qualification-only overlay in integration/mobile_flow. Existing perf TestMain
// and runParallelSimulation retain complete process workers and verification.
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"regexp"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/integration/mobile_flow/config"
)

type qualificationLoadControl struct {
	users, concurrency, seconds int
	nonce                       string
}

func readQualificationLoadControl() (qualificationLoadControl, error) {
	var control qualificationLoadControl
	fields := []struct {
		name        string
		destination *int
	}{
		{"OVERSQLITE_LOAD_USERS", &control.users},
		{"OVERSQLITE_LOAD_CONCURRENCY", &control.concurrency},
		{"OVERSQLITE_LOAD_SECONDS", &control.seconds},
	}
	for _, field := range fields {
		raw := os.Getenv(field.name)
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || strconv.Itoa(value) != raw {
			return control, fmt.Errorf("invalid owned load control %s", field.name)
		}
		*field.destination = value
	}
	seconds, allowed := map[int]int{1: 220, 5: 220, 10: 220, 25: 280}[control.users]
	if !allowed || control.concurrency != control.users || control.seconds != seconds {
		return control, fmt.Errorf("load phase differs from the approved1/1,5/5,10/10,25/25 ladder")
	}
	control.nonce = os.Getenv("OVERSQLITE_LOAD_NONCE")
	if !regexp.MustCompile(`^[a-f0-9]{32}$`).MatchString(control.nonce) {
		return control, fmt.Errorf("missing exact load-sequence nonce")
	}
	return control, nil
}

func TestQualificationLoadCompleteScenario(t *testing.T) {
	control, err := readQualificationLoadControl()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL", "OVERSYNC_PERF_DATABASE_URL", "JWT_SECRET"} {
		if os.Getenv(name) == "" {
			t.Fatalf("missing private owned load configuration %s", name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(control.seconds)*time.Second)
	defer cancel()
	t.Setenv(perfProcessWorkerEnvironment, "1")
	cfg := &config.Config{
		ServerURL: os.Getenv("OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL"), DatabaseURL: os.Getenv("OVERSYNC_PERF_DATABASE_URL"),
		JWTSecret: os.Getenv("JWT_SECRET"), EnableVerify: true, PreserveDB: true,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	started := time.Now()
	t.Logf("QUALIFICATION_LOAD_START nonce=%s pid=%d pgid=%d", control.nonce, os.Getpid(), syscall.Getpgrp())
	if err := runParallelSimulation(ctx, cfg, "complex-multi-batch", control.users, control.concurrency); err != nil {
		t.Fatalf("complete verified process-worker scenario failed: %v", err)
	}
	t.Logf("QUALIFICATION_LOAD_COMPLETE nonce=%s users=%d concurrency=%d verify=true scenario=complex-multi-batch mode=process elapsed_ns=%d", control.nonce, control.users, control.concurrency, time.Since(started).Nanoseconds())
}

func TestQualificationLoadControls(t *testing.T) {
	const nonce = "0123456789abcdef0123456789abcdef"
	t.Setenv("OVERSQLITE_LOAD_NONCE", nonce)
	for users, seconds := range map[int]int{1: 220, 5: 220, 10: 220, 25: 280} {
		t.Setenv("OVERSQLITE_LOAD_USERS", strconv.Itoa(users))
		t.Setenv("OVERSQLITE_LOAD_CONCURRENCY", strconv.Itoa(users))
		t.Setenv("OVERSQLITE_LOAD_SECONDS", strconv.Itoa(seconds))
		control, err := readQualificationLoadControl()
		if err != nil || control.users != users || control.concurrency != users || control.seconds != seconds || control.nonce != nonce {
			t.Fatalf("valid phase rejected: %+v %v", control, err)
		}
	}
	for _, values := range [][4]string{
		{"100", "25", "280", nonce}, {"25", "100", "280", nonce},
		{"25", "10", "280", nonce}, {"25", "25", "1200", nonce},
		{"01", "1", "220", nonce}, {"1 ", "1", "220", nonce},
		{"1", "1", "220", "missing"}, {"1", "1", "280", nonce},
	} {
		for index, name := range []string{"OVERSQLITE_LOAD_USERS", "OVERSQLITE_LOAD_CONCURRENCY", "OVERSQLITE_LOAD_SECONDS", "OVERSQLITE_LOAD_NONCE"} {
			t.Setenv(name, values[index])
		}
		if _, err := readQualificationLoadControl(); err == nil {
			t.Fatalf("invalid phase accepted: %q", values)
		}
	}
}
