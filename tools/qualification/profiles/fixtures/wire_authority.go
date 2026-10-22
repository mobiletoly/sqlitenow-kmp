// Qualification-only rich authority and deterministic production-encoded inputs.
// Compile this named source with the separately bound oversync facade overlay.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/integration/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversync"
)

type wireInput struct {
	ID            string                      `json:"id"`
	Rows          []oversync.PushRequestRow   `json:"rows"`
	Requests      []json.RawMessage           `json:"compact_requests"`
	RequestSHA256 []string                    `json:"compact_request_sha256"`
	Bundles       [][]oversync.PushRequestRow `json:"bundles,omitempty"`
	ExpectedRows  int                         `json:"expected_rows"`
	RowsSHA256    string                      `json:"rows_sha256"`
}

type wireManifest struct {
	Version           int                                   `json:"schema_version"`
	DefinitionsSHA256 string                                `json:"definitions_sha256"`
	Generation        oversync.SchemaGenerationCapabilities `json:"generation"`
	DDL               string                                `json:"sqlite_ddl"`
	Workloads         []wireInput                           `json:"workloads"`
	OpaqueSHA256      string                                `json:"opaque_sha256"`
}

func wireSHA(raw []byte) string { value := sha256.Sum256(raw); return hex.EncodeToString(value[:]) }
func wireID(ordinal int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012x", ordinal) }

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() (result error) {
	if len(os.Args) == 4 && os.Args[1] == "--capture-variants" {
		return captureWireVariants(os.Args[2], os.Args[3])
	}
	if len(os.Args) == 4 && os.Args[1] == "--write-manifest" {
		return writeWireManifest(os.Args[2], os.Args[3])
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if len(os.Args) == 2 && os.Args[1] == "--cleanup" {
		cleanup, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		return server.CleanupSmokeDatabase(cleanup)
	}
	if len(os.Args) != 1 {
		return errors.New("unexpected wire authority arguments")
	}
	for _, name := range []string{"JWT_SECRET", "OVERSYNC_EXAMPLE_TEST_DATABASE_URL", "OVERSYNC_SMOKE_DATABASE_NAME", "OVERSYNC_QUALIFICATION_READY_FILE", "OVERSYNC_QUALIFICATION_READY_NONCE", "OVERSQLITE_PROCESS_OWNER", "OVERSQLITE_WIRE_WORKLOAD_MANIFEST", "OVERSQLITE_WIRE_ARTIFACT_OUTPUT", "OVERSQLITE_WIRE_RUNTIME"} {
		if os.Getenv(name) == "" {
			return fmt.Errorf("missing private wire control %s", name)
		}
	}
	for _, name := range []string{"OVERSYNC_MAX_CONCURRENT_HISTORY_PROJECTIONS", "OVERSYNC_DB_POOL_MAX_CONNS", "OVERSYNC_DB_POOL_MIN_CONNS", "OVERSYNC_MAX_CONCURRENT_SNAPSHOT_BUILDS", "OVERSYNC_MAX_CONCURRENT_SNAPSHOT_CHUNK_REQUESTS"} {
		if os.Getenv(name) != "" {
			return fmt.Errorf("wire fixture refuses default override %s", name)
		}
	}
	manifestPath := os.Getenv("OVERSQLITE_WIRE_WORKLOAD_MANIFEST")
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		return err
	}
	var manifest wireManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return err
	}
	if manifest.Version != 1 || len(manifest.Workloads) != 7 || manifest.Generation.ID != "1" {
		return errors.New("incomplete wire input manifest")
	}
	authority, err := server.NewTestServer(&server.ServerConfig{AppName: "oversqlite-qualification-wire", JWTSecret: os.Getenv("JWT_SECRET"), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, authority.CloseWithError()) }()
	generation, err := authority.SyncService.GetGeneration(ctx, oversync.Actor{UserID: "qualification-readiness", SourceID: "qualification-readiness"}, 1)
	if err != nil {
		return err
	}
	if generation.SchemaGeneration.Fingerprint != manifest.Generation.Fingerprint {
		return errors.New("wire manifest generation differs from live rich authority")
	}
	nonce := os.Getenv("OVERSYNC_QUALIFICATION_READY_NONCE")
	var snapshotSeed map[string]any
	if runtime := os.Getenv("OVERSQLITE_WIRE_RUNTIME"); runtime == "browser" || runtime == "wasm" {
		seedCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		snapshotSeed, err = seedWireSnapshot(seedCtx, authority, manifest, nonce)
		cancel()
		if err != nil {
			return err
		}
		if err := writeQualificationReadiness(filepath.Join(os.Getenv("OVERSQLITE_WIRE_ARTIFACT_OUTPUT"), "snapshot-seed.json"), snapshotSeed); err != nil {
			return err
		}
	}
	handler, err := wireHandler(authority.Handler, raw, manifest, nonce, os.Getenv("OVERSQLITE_WIRE_ARTIFACT_OUTPUT"), snapshotSeed)
	if err != nil {
		return err
	}
	previous := authority.HTTPServer
	authority.HTTPServer = httptest.NewServer(handler)
	previous.Close()
	details := map[string]any{"manifest_sha256": wireSHA(raw), "workloads": 7, "product_defaults": true}
	if snapshotSeed != nil {
		details["snapshot_seed"] = snapshotSeed
	}
	if err := qualificationReadiness(ctx, "wire", authority.HTTPServer.URL, authority, details); err != nil {
		return err
	}
	fmt.Println("QUALIFICATION_WIRE_AUTHORITY_PROFILE workloads=7 product_defaults=true")
	fmt.Printf("OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL=%s\n", authority.HTTPServer.URL)
	<-ctx.Done()
	return nil
}

// Fixture preparation only. The measured web follower still performs the full
// default-limit snapshot download, worker apply and typed-state verification.
func seedWireSnapshot(ctx context.Context, authority *server.TestServer, manifest wireManifest, nonce string) (map[string]any, error) {
	started := time.Now()
	input := manifest.Workloads[5]
	if input.ID != "large-snapshot-chunk" || len(input.Rows) != 1000 || len(input.Bundles) != 5 {
		return nil, errors.New("incomplete snapshot seed inputs")
	}
	user := "wire-" + nonce + "-5"
	manager := oversync.NewScopeManager(authority.SyncService, oversync.ScopeManagerConfig{})
	var bundles []oversync.CommittedBundleRef
	validUntil := time.Now().UTC().Truncate(time.Microsecond).Add(10 * time.Minute)
	for index, rows := range input.Bundles {
		if len(rows) != 200 {
			return nil, errors.New("snapshot seed batch differs from fixed inputs")
		}
		encoded, err := json.Marshal(rows)
		if err != nil {
			return nil, err
		}
		opts := oversync.ScopeWriteOptions{WriterID: "qualification-wire-snapshot", RetryableWriteOptions: oversync.RetryableWriteOptions{
			OperationID:   uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("%s-snapshot-%d", nonce, index))),
			OperationHash: sha256.Sum256(encoded), OperationValidUntil: validUntil,
		}}
		result, err := manager.ExecWrite(ctx, user, opts, func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
			for _, row := range rows {
				var payload struct {
					ID       string  `json:"id"`
					Name     string  `json:"name"`
					ParentID *string `json:"parent_id"`
				}
				if row.Schema != "business" || row.Table != "categories" || row.Op != oversync.OpInsert {
					return errors.New("unexpected snapshot seed row")
				}
				if err := json.Unmarshal(row.Payload, &payload); err != nil {
					return err
				}
				if _, err := tx.Exec(ctx, `INSERT INTO business.categories(id,name,parent_id) VALUES($1,$2,$3)`, payload.ID, payload.Name, payload.ParentID); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		if result.Bundle.RowCount != 200 {
			return nil, errors.New("snapshot seed server-write effect count differs")
		}
		bundles = append(bundles, result.Bundle)
	}
	encoded, err := json.Marshal(input.Rows)
	if err != nil {
		return nil, err
	}
	return map[string]any{"nonce": nonce, "user": user, "rows": 1000, "batches": 5, "rows_sha256": wireSHA(encoded),
		"route": "ScopeManager.ExecWrite", "native_credit": 0, "bundles": bundles, "elapsed_seconds": time.Since(started).Seconds()}, nil
}

func captureWireVariants(input, output string) error {
	raw, err := os.ReadFile(input)
	if err != nil {
		return err
	}
	var items []struct {
		ID   string `json:"id"`
		Path string `json:"path"`
		SHA  string `json:"sha256"`
	}
	if err := json.Unmarshal(raw, &items); err != nil {
		return err
	}
	if len(items) == 0 || len(items) > 1000 {
		return errors.New("invalid wire comparison inventory")
	}
	if err := os.Mkdir(output, 0700); err != nil {
		return err
	}
	var records []map[string]any
	for _, item := range items {
		if !regexp.MustCompile(`^[0-9]{6}-(request|response)$`).MatchString(item.ID) || !filepath.IsAbs(item.Path) {
			return errors.New("invalid wire variant identity")
		}
		file, err := os.Open(item.Path)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(io.LimitReader(file, (64<<20)+1))
		err = errors.Join(readErr, file.Close())
		if err != nil {
			return err
		}
		if len(body) > 64<<20 || wireSHA(body) != item.SHA {
			return errors.New("wire variant source changed")
		}
		selected, comparison, err := oversync.QualificationWireVariants(body)
		if err != nil {
			return err
		}
		files := map[string][]byte{"identity": body, "gzip-comparison": comparison, "production-selected": selected}
		metadata := map[string]any{"id": item.ID, "source_sha256": item.SHA, "files": map[string]any{}}
		for kind, value := range files {
			path := filepath.Join(output, item.ID+"-"+kind+".bin")
			stream, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if err != nil {
				return err
			}
			_, writeErr := stream.Write(value)
			if err := errors.Join(writeErr, stream.Close()); err != nil {
				return err
			}
			metadata["files"].(map[string]any)[kind] = map[string]any{"path": path, "bytes": len(value), "sha256": wireSHA(value)}
		}
		records = append(records, metadata)
	}
	return writeQualificationReadiness(filepath.Join(output, "manifest.json"), records)
}

func wireHandler(next http.Handler, raw []byte, manifest wireManifest, nonce, output string, snapshotSeed map[string]any) (http.Handler, error) {
	info, err := os.Stat(output)
	if err != nil {
		return nil, err
	}
	canonical, err := filepath.EvalSymlinks(output)
	if err != nil {
		return nil, err
	}
	if !filepath.IsAbs(output) || canonical != output || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("wire receipts require an owned canonical private directory")
	}
	allowed := map[string]bool{}
	for _, input := range manifest.Workloads {
		allowed[input.ID] = true
	}
	var lock sync.Mutex
	seen := map[string]bool{}
	handler := server.BrowserTestCORSMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Expose-Headers", "X-Qualification-Ordinal, X-Qualification-Nonce")
		switch r.Method + " " + r.URL.Path {
		case "GET /qualification/wire/incompressible":
			oversync.QualificationWriteWireIncompressible(w, r)
		case "GET /qualification/wire/manifest":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"nonce": nonce, "manifest_sha256": wireSHA(raw), "manifest": json.RawMessage(raw), "runtime": os.Getenv("OVERSQLITE_WIRE_RUNTIME"), "snapshot_seed": snapshotSeed})
		case "POST /qualification/wire/receipt":
			var receipt struct {
				Nonce    string          `json:"nonce"`
				Runtime  string          `json:"runtime"`
				ID       string          `json:"workload"`
				Evidence json.RawMessage `json:"evidence"`
			}
			body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
			if err != nil {
				http.Error(w, "receipt bound", 400)
				return
			}
			decoder := json.NewDecoder(strings.NewReader(string(body)))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&receipt); err != nil || receipt.Nonce != nonce || !allowed[receipt.ID] || (receipt.Runtime != "raw" && receipt.Runtime != "browser" && receipt.Runtime != "wasm") || receipt.Runtime != os.Getenv("OVERSQLITE_WIRE_RUNTIME") || len(receipt.Evidence) == 0 {
				http.Error(w, "invalid wire receipt", 400)
				return
			}
			lock.Lock()
			defer lock.Unlock()
			if seen[receipt.ID] {
				http.Error(w, "duplicate wire receipt", 409)
				return
			}
			path := filepath.Join(output, fmt.Sprintf("receipt-%02d.json", len(seen)+1))
			if err := writeQualificationReadiness(path, receipt); err != nil {
				http.Error(w, "receipt publication failed", 500)
				return
			}
			seen[receipt.ID] = true
			w.WriteHeader(http.StatusNoContent)
		default:
			next.ServeHTTP(w, r)
		}
	}))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", strings.Join([]string{"Authorization", "Content-Encoding", "Content-Type", oversync.ProtocolVersionHeader, oversync.SourceIDHeader, oversync.SchemaGenerationIDHeader, oversync.SchemaGenerationFingerprintHeader, "X-Qualification-Workload", "X-Qualification-Stage"}, ", "))
			w.Header().Set("Access-Control-Max-Age", "600")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		handler.ServeHTTP(w, r)
	}), nil
}

func writeWireManifest(definitions, destination string) error {
	raw, err := os.ReadFile(definitions)
	if err != nil {
		return err
	}
	var input struct {
		Version   int `json:"schema_version"`
		Workloads []struct {
			ID string `json:"id"`
		} `json:"workloads"`
	}
	if err := json.Unmarshal(raw, &input); err != nil {
		return err
	}
	if input.Version != 1 || len(input.Workloads) != 7 {
		return errors.New("expected seven fixed wire definitions")
	}
	registry, err := oversync.CompileSchemaGenerationRegistry(server.RegisteredTablesForBusinessSchema("business"), server.SchemaGenerationsForBusinessSchema("business"))
	if err != nil {
		return err
	}
	contract, ok := registry.Generation(1)
	if !ok {
		return errors.New("missing rich generation1")
	}
	manifest := wireManifest{Version: 1, DefinitionsSHA256: wireSHA(raw), Generation: oversync.QualificationWireGeneration(contract), OpaqueSHA256: wireSHA(oversync.QualificationWireIncompressibleBody())}
	manifest.DDL, err = wireDDL(manifest.Generation)
	if err != nil {
		return err
	}
	for ordinal, definition := range input.Workloads {
		workload, err := makeWireInput(definition.ID)
		if err != nil {
			return err
		}
		rowsJSON, err := json.Marshal(workload.Rows)
		if err != nil {
			return err
		}
		workload.RowsSHA256 = wireSHA(rowsJSON)
		groups := workload.Bundles
		if len(groups) == 0 && len(workload.Rows) > 0 {
			groups = [][]oversync.PushRequestRow{workload.Rows}
		}
		for index, rows := range groups {
			encoded, err := oversync.QualificationWirePush(contract, int64(index+1), rows)
			if err != nil {
				return err
			}
			workload.Requests = append(workload.Requests, json.RawMessage(encoded))
			workload.RequestSHA256 = append(workload.RequestSHA256, wireSHA(encoded))
		}
		if ordinal > 0 && workload.ID == manifest.Workloads[0].ID {
			return errors.New("duplicate wire definition")
		}
		manifest.Workloads = append(manifest.Workloads, workload)
	}
	return writeQualificationReadiness(destination, manifest)
}

func makeWireInput(id string) (wireInput, error) {
	result := wireInput{ID: id}
	const timestamp = "2026-01-01T00:00:00Z"
	category := func(ordinal, width int) oversync.PushRequestRow {
		name := "a"
		if width > 1 {
			name = strings.Repeat("A", width)
		}
		payload, _ := json.Marshal(map[string]any{"id": wireID(ordinal), "name": name, "parent_id": nil})
		return oversync.PushRequestRow{Schema: "business", Table: "categories", Key: oversync.SyncKey{"id": wireID(ordinal)}, Op: oversync.OpInsert, Payload: payload}
	}
	switch id {
	case "small-single-row-insert":
		result.Rows = []oversync.PushRequestRow{category(0, 1)}
		result.ExpectedRows = 1
	case "small-single-row-delete":
		result.Rows = []oversync.PushRequestRow{{Schema: "business", Table: "categories", Key: oversync.SyncKey{"id": wireID(0)}, Op: oversync.OpDelete, BaseRowVersion: 1}}
		result.ExpectedRows = 0
	case "two-hundred-one-table-push":
		for i := 0; i < 200; i++ {
			result.Rows = append(result.Rows, category(i, 128))
		}
		result.ExpectedRows = 200
	case "mixed-table-push":
		payload, _ := json.Marshal(map[string]any{"id": wireID(0), "name": "a", "email": "a@example.invalid", "created_at": timestamp, "updated_at": timestamp})
		result.Rows = append(result.Rows, oversync.PushRequestRow{Schema: "business", Table: "users", Key: oversync.SyncKey{"id": wireID(0)}, Op: oversync.OpInsert, Payload: payload})
		for i := 1; i <= 200; i++ {
			payload, _ := json.Marshal(map[string]any{"id": wireID(i), "author_id": wireID(0), "title": "a", "content": strings.Repeat("A", 128), "created_at": timestamp, "updated_at": timestamp})
			result.Rows = append(result.Rows, oversync.PushRequestRow{Schema: "business", Table: "posts", Key: oversync.SyncKey{"id": wireID(i)}, Op: oversync.OpInsert, Payload: payload})
		}
		result.ExpectedRows = 201
	case "ordinary-multi-bundle-pull":
		for batch := 0; batch < 3; batch++ {
			var rows []oversync.PushRequestRow
			for i := 0; i < 100; i++ {
				rows = append(rows, category(batch*100+i, 128))
			}
			result.Bundles = append(result.Bundles, rows)
		}
		result.ExpectedRows = 300
	case "large-snapshot-chunk":
		for i := 0; i < 1000; i++ {
			result.Rows = append(result.Rows, category(i, 1024))
		}
		for offset := 0; offset < len(result.Rows); offset += 200 {
			result.Bundles = append(result.Bundles, append([]oversync.PushRequestRow(nil), result.Rows[offset:offset+200]...))
		}
		result.ExpectedRows = 1000
	case "deterministic-incompressible-above-threshold":
	default:
		return result, fmt.Errorf("unknown fixed wire workload %q", id)
	}
	return result, nil
}

func wireDDL(generation oversync.SchemaGenerationCapabilities) (string, error) {
	var statements []string
	types := map[string]string{"boolean": "INTEGER", "int64": "INTEGER", "decimal": "TEXT", "float64": "REAL", "text": "TEXT", "bytes": "BLOB", "uuid": "TEXT", "json": "TEXT"}
	quoteList := func(names []string) string {
		var quoted []string
		for _, name := range names {
			quoted = append(quoted, fmt.Sprintf("%q", name))
		}
		return strings.Join(quoted, ",")
	}
	for _, table := range generation.Tables {
		var columns []string
		for _, column := range table.Columns {
			kind, ok := types[string(column.WireType)]
			if !ok {
				return "", fmt.Errorf("unknown wire type %s", column.WireType)
			}
			null := " NOT NULL"
			if column.Nullable {
				null = ""
			}
			columns = append(columns, fmt.Sprintf("%q %s%s", column.Name, kind, null))
		}
		columns = append(columns, "PRIMARY KEY ("+quoteList(table.SyncKeyColumns)+")")
		for _, fk := range table.ForeignKeys {
			columns = append(columns, fmt.Sprintf("FOREIGN KEY (%s) REFERENCES %q (%s) ON DELETE %s ON UPDATE %s DEFERRABLE INITIALLY DEFERRED", quoteList(fk.Columns), fk.ReferencedTable, quoteList(fk.ReferencedColumns), strings.ReplaceAll(strings.ToUpper(string(fk.OnDelete)), "_", " "), strings.ReplaceAll(strings.ToUpper(string(fk.OnUpdate)), "_", " ")))
		}
		statements = append(statements, fmt.Sprintf("CREATE TABLE %q (%s);", table.Table, strings.Join(columns, ",")))
	}
	return strings.Join(statements, "\n"), nil
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

func writeQualificationReadiness(path string, record any) error {
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
