// Qualification-only load authority; compile this named file explicitly.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"syscall"
	"time"

	"github.com/mobiletoly/go-oversync/integration/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversync"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() (result error) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if len(os.Args) == 2 && os.Args[1] == "--cleanup" {
		cleanup, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		return server.CleanupSmokeDatabase(cleanup)
	}
	if len(os.Args) != 1 || os.Getenv("JWT_SECRET") == "" || os.Getenv("OVERSYNC_EXAMPLE_TEST_DATABASE_URL") == "" || os.Getenv("OVERSYNC_SMOKE_DATABASE_NAME") == "" {
		return errors.New("owned load authority requires its private synthetic environment")
	}
	for _, name := range []string{"OVERSYNC_QUALIFICATION_READY_FILE", "OVERSYNC_QUALIFICATION_READY_NONCE", "OVERSQLITE_PROCESS_OWNER"} {
		if os.Getenv(name) == "" {
			return fmt.Errorf("missing private load authority control %s", name)
		}
	}
	for _, name := range []string{"OVERSYNC_MAX_CONCURRENT_HISTORY_PROJECTIONS", "OVERSYNC_DB_POOL_MAX_CONNS", "OVERSYNC_DB_POOL_MIN_CONNS", "OVERSYNC_MAX_CONCURRENT_SNAPSHOT_BUILDS", "OVERSYNC_MAX_CONCURRENT_SNAPSHOT_CHUNK_REQUESTS"} {
		if os.Getenv(name) != "" {
			return fmt.Errorf("unexpected load configuration override %s", name)
		}
	}
	authority, err := server.NewTestServer(&server.ServerConfig{
		AppName: "oversqlite-qualification-load", JWTSecret: os.Getenv("JWT_SECRET"),
		MaxConcurrentHistoryProjections: 32,
		Logger:                          slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, authority.CloseWithError()) }()
	if err := qualificationReadiness(ctx, "load", authority.HTTPServer.URL, authority, map[string]any{"history_projection_concurrency": 32, "product_default": 4}); err != nil {
		return err
	}
	fmt.Println("QUALIFICATION_LOAD_AUTHORITY_PROFILE history_projections=32 product_default=4 max_allowed=32")
	fmt.Printf("OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL=%s\n", authority.HTTPServer.URL)
	<-ctx.Done()
	return nil
}

// qualificationReadiness is opt-in. Ordinary fixture output and lifecycle stay
// unchanged; the coordinator consumes this private receipt before any client.
func qualificationReadiness(ctx context.Context, profile, base string, authority *server.TestServer, details map[string]any) error {
	if os.Getenv("OVERSYNC_QUALIFICATION_READY_FILE") == "" {
		return nil
	}
	generation, err := authority.SyncService.GetGeneration(ctx, oversync.Actor{UserID: "qualification-readiness", SourceID: "qualification-readiness"}, 1)
	if err != nil {
		return fmt.Errorf("qualification generation readiness: %w", err)
	}
	return publishQualificationReadiness(profile, base, authority.Pool.Config().ConnConfig.Database, generation, details)
}

func publishQualificationReadiness(profile, base, database string, generation any, details map[string]any) error {
	path := os.Getenv("OVERSYNC_QUALIFICATION_READY_FILE")
	if path == "" {
		return nil
	}
	nonce, owner := os.Getenv("OVERSYNC_QUALIFICATION_READY_NONCE"), os.Getenv("OVERSQLITE_PROCESS_OWNER")
	if !regexp.MustCompile(`^[a-f0-9]{32}$`).MatchString(nonce) || !regexp.MustCompile(`^oversqlite-[a-f0-9]{32}$`).MatchString(owner) {
		return errors.New("private readiness requires exact owner and nonce")
	}
	if profile == "" || base == "" || database == "" || generation == nil {
		return errors.New("incomplete private readiness identity")
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	binary, err := os.Open(executable)
	if err != nil {
		return err
	}
	digest := sha256.New()
	_, copyErr := io.Copy(digest, binary)
	if err := errors.Join(copyErr, binary.Close()); err != nil {
		return err
	}
	record := map[string]any{
		"schema_version": 1, "nonce": nonce, "owner": owner,
		"pid": os.Getpid(), "pgid": syscall.Getpgrp(), "profile": profile,
		"authority_url": base, "database": database, "generation": generation,
		"fixture_sha256": fmt.Sprintf("%x", digest.Sum(nil)),
		"reset_domain":   nonce, "created_unix_ns": time.Now().UnixNano(), "details": details,
	}
	return writeQualificationReadiness(path, record)
}

func writeQualificationReadiness(path string, record map[string]any) error {
	if !filepath.IsAbs(path) {
		return errors.New("private readiness needs an absolute path")
	}
	parent := filepath.Dir(path)
	info, err := os.Stat(parent)
	if err != nil {
		return err
	}
	canonical, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 || canonical != parent {
		return errors.New("private readiness parent must be a canonical private directory")
	}
	file, err := os.CreateTemp(parent, ".readiness-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	encodeErr := json.NewEncoder(file).Encode(record)
	syncErr := file.Sync()
	if err := errors.Join(encodeErr, syncErr, file.Close()); err != nil {
		return err
	}
	// A hard link atomically publishes a complete file and refuses an existing
	// destination, including a stale file or symlink; rename would overwrite it.
	if err := os.Link(file.Name(), path); err != nil {
		return fmt.Errorf("publish private readiness: %w", err)
	}
	return nil
}
