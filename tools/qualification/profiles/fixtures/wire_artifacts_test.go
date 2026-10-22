// Qualification-only overlay in integration/nethttp_server/server.
package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

type qualificationWireInput struct {
	ID            string                      `json:"id"`
	Rows          []oversync.PushRequestRow   `json:"rows"`
	Requests      []json.RawMessage           `json:"compact_requests"`
	RequestSHA256 []string                    `json:"compact_request_sha256"`
	Bundles       [][]oversync.PushRequestRow `json:"bundles"`
	ExpectedRows  int                         `json:"expected_rows"`
}
type qualificationWireManifest struct {
	Version      int                                   `json:"schema_version"`
	Generation   oversync.SchemaGenerationCapabilities `json:"generation"`
	DDL          string                                `json:"sqlite_ddl"`
	Workloads    []qualificationWireInput              `json:"workloads"`
	OpaqueSHA256 string                                `json:"opaque_sha256"`
}
type qualificationWireExchange struct {
	Ordinal               int             `json:"ordinal"`
	Nonce                 string          `json:"nonce"`
	Workload              string          `json:"workload"`
	Stage                 string          `json:"stage"`
	Route                 string          `json:"route"`
	Status                int             `json:"status"`
	RequestDecodedSHA256  string          `json:"request_decoded_sha256"`
	ResponseDecodedSHA256 string          `json:"response_decoded_sha256"`
	Request               json.RawMessage `json:"-"`
	Response              json.RawMessage `json:"-"`
}
type qualificationWireTransport struct {
	base            *http.Transport
	lock            sync.Mutex
	workload, stage string
	exchanges       []qualificationWireExchange
}

func TestQualificationWireEncoderControls(t *testing.T) {
	for _, body := range [][]byte{[]byte(`{"a":1}`), []byte(strings.Repeat("A", 4096)), oversync.QualificationWireIncompressibleBody()} {
		selected, comparison, err := oversync.QualificationWireVariants(body)
		require.NoError(t, err)
		decoded, err := qualificationWireDecode(comparison, "gzip")
		require.NoError(t, err)
		require.Equal(t, body, decoded)
		if len(body) >= 2048 && len(comparison) < len(body) {
			require.Equal(t, comparison, selected)
		} else {
			require.Equal(t, body, selected)
		}
	}
	registry, err := oversync.CompileSchemaGenerationRegistry(RegisteredTablesForBusinessSchema("business"), SchemaGenerationsForBusinessSchema("business"))
	require.NoError(t, err)
	contract, ok := registry.Generation(1)
	require.True(t, ok)
	rows := []oversync.PushRequestRow{{Schema: "business", Table: "categories", Key: oversync.SyncKey{"id": "00000000-0000-4000-8000-000000000000"}, Op: oversync.OpInsert, Payload: json.RawMessage(`{"id":"00000000-0000-4000-8000-000000000000","name":"a","parent_id":null}`)}}
	encoded, err := oversync.QualificationWirePush(contract, 1, rows)
	require.NoError(t, err)
	var request oversync.CompactPushRequest
	require.NoError(t, json.Unmarshal(encoded, &request))
	require.Len(t, request.CanonicalRequestHash, 64)
	expanded, err := oversync.ExpandCompactPushRows(oversync.QualificationWireGeneration(contract), request.Rows)
	require.NoError(t, err)
	require.Equal(t, rows, expanded)
}

func qualificationWireSHA(raw []byte) string {
	value := sha256.Sum256(raw)
	return hex.EncodeToString(value[:])
}
func qualificationWireDecode(raw []byte, encoding string) ([]byte, error) {
	if encoding == "" || encoding == "identity" {
		return raw, nil
	}
	if encoding != "gzip" {
		return nil, fmt.Errorf("unknown wire encoding %q", encoding)
	}
	reader, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	value, err := io.ReadAll(io.LimitReader(reader, (64<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(value) > 64<<20 {
		return nil, fmt.Errorf("decoded wire entity exceeds64MiB")
	}
	return value, nil
}
func (t *qualificationWireTransport) phase(workload, stage string) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.workload, t.stage = workload, stage
}
func (t *qualificationWireTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	t.lock.Lock()
	workload, stage := t.workload, t.stage
	t.lock.Unlock()
	request.Header.Set("X-Qualification-Workload", workload)
	request.Header.Set("X-Qualification-Stage", stage)
	var raw []byte
	var err error
	if request.Body != nil {
		raw, err = io.ReadAll(io.LimitReader(request.Body, (64<<20)+1))
		if err != nil {
			return nil, err
		}
		request.Body.Close()
		request.Body = io.NopCloser(bytes.NewReader(raw))
	}
	if len(raw) > 64<<20 {
		return nil, fmt.Errorf("request entity exceeds64MiB")
	}
	response, err := t.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	encoded, err := io.ReadAll(io.LimitReader(response.Body, (64<<20)+1))
	response.Body.Close()
	if err != nil {
		return nil, err
	}
	if len(encoded) > 64<<20 {
		return nil, fmt.Errorf("response entity exceeds64MiB")
	}
	response.Body = io.NopCloser(bytes.NewReader(encoded))
	if !strings.HasPrefix(request.URL.Path, "/sync/") && request.URL.Path != "/qualification/wire/incompressible" {
		return response, nil
	}
	decodedRequest, err := qualificationWireDecode(raw, request.Header.Get("Content-Encoding"))
	if err != nil {
		return nil, err
	}
	decodedResponse, err := qualificationWireDecode(encoded, response.Header.Get("Content-Encoding"))
	if err != nil {
		return nil, err
	}
	ordinal, err := strconv.Atoi(response.Header.Get("X-Qualification-Ordinal"))
	if err != nil || ordinal < 1 {
		return nil, fmt.Errorf("missing actual capture ordinal")
	}
	nonce := response.Header.Get("X-Qualification-Nonce")
	if len(nonce) != 32 {
		return nil, fmt.Errorf("missing actual capture nonce")
	}
	row := qualificationWireExchange{Ordinal: ordinal, Nonce: nonce, Workload: workload, Stage: stage, Route: request.URL.Path, Status: response.StatusCode,
		RequestDecodedSHA256: qualificationWireSHA(decodedRequest), ResponseDecodedSHA256: qualificationWireSHA(decodedResponse), Request: decodedRequest, Response: decodedResponse}
	t.lock.Lock()
	t.exchanges = append(t.exchanges, row)
	t.lock.Unlock()
	return response, nil
}

func TestQualificationWireByteArtifacts(t *testing.T) {
	require.Equal(t, "true", os.Getenv("OVERSQLITE_WIRE_ARTIFACT_TESTS"), "explicit isolated wire activation required")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	base, output := os.Getenv("OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL"), os.Getenv("OVERSQLITE_WIRE_ARTIFACT_OUTPUT")
	require.NotEmpty(t, base)
	require.True(t, filepath.IsAbs(output))
	info, err := os.Stat(output)
	require.NoError(t, err)
	require.Zero(t, info.Mode().Perm()&0077)
	manifestRaw, err := os.ReadFile(os.Getenv("OVERSQLITE_WIRE_WORKLOAD_MANIFEST"))
	require.NoError(t, err)
	var manifest qualificationWireManifest
	require.NoError(t, json.Unmarshal(manifestRaw, &manifest))
	require.Len(t, manifest.Workloads, 7)
	transport := &qualificationWireTransport{base: http.DefaultTransport.(*http.Transport).Clone()}
	transport.base.DisableCompression = true
	defer transport.base.CloseIdleConnections()
	httpClient := &http.Client{Transport: transport, Timeout: 30 * time.Second}
	var envelope struct {
		Nonce string `json:"nonce"`
		SHA   string `json:"manifest_sha256"`
	}
	response, err := httpClient.Get(base + "/qualification/wire/manifest")
	require.NoError(t, err)
	require.NoError(t, json.NewDecoder(response.Body).Decode(&envelope))
	require.NoError(t, response.Body.Close())
	require.Equal(t, qualificationWireSHA(manifestRaw), envelope.SHA)
	for index, input := range manifest.Workloads {
		t.Run(input.ID, func(t *testing.T) {
			transport.phase(input.ID, "prepare")
			if input.ID == "deterministic-incompressible-above-threshold" {
				transport.phase(input.ID, "measure")
				request, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/qualification/wire/incompressible", nil)
				require.NoError(t, err)
				request.Header.Set("Accept-Encoding", "gzip")
				response, err := httpClient.Do(request)
				require.NoError(t, err)
				raw, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				require.NoError(t, response.Body.Close())
				require.Equal(t, 200, response.StatusCode)
				require.Empty(t, response.Header.Get("Content-Encoding"))
				require.Len(t, raw, 4096)
				require.Equal(t, manifest.OpaqueSHA256, qualificationWireSHA(raw))
			} else {
				user := fmt.Sprintf("wire-%s-%02d", envelope.Nonce, index)
				writer := qualificationWireClient(t, ctx, base, output, user+"-writer", user, manifest, httpClient)
				defer writer.Close()
				var follower *oversqlite.Client
				if input.ID == "ordinary-multi-bundle-pull" {
					follower = qualificationWireClient(t, ctx, base, output, user+"-follower", user, manifest, httpClient)
					defer follower.Close()
					_, err = follower.PullToStable(ctx)
					require.NoError(t, err)
				}
				if input.ID == "small-single-row-delete" {
					var initial qualificationWireInput
					for _, candidate := range manifest.Workloads {
						if candidate.ID == "small-single-row-insert" {
							initial = candidate
						}
					}
					qualificationWireApply(t, ctx, writer.Database(), initial.Rows)
					_, err = writer.PushPending(ctx)
					require.NoError(t, err)
				}
				if input.ID == "ordinary-multi-bundle-pull" {
					for _, rows := range input.Bundles {
						qualificationWireApply(t, ctx, writer.Database(), rows)
						_, err = writer.PushPending(ctx)
						require.NoError(t, err)
					}
					transport.phase(input.ID, "measure")
					_, err = follower.PullToStable(ctx)
					require.NoError(t, err)
					var expected []oversync.PushRequestRow
					for _, rows := range input.Bundles {
						expected = append(expected, rows...)
					}
					qualificationWireAssertState(t, ctx, follower.Database(), manifest.Generation, expected)
				} else {
					if input.ID == "large-snapshot-chunk" {
						for _, rows := range input.Bundles {
							qualificationWireApply(t, ctx, writer.Database(), rows)
							_, err = writer.PushPending(ctx)
							require.NoError(t, err)
						}
					} else {
						qualificationWireApply(t, ctx, writer.Database(), input.Rows)
						transport.phase(input.ID, "measure")
						_, err = writer.PushPending(ctx)
						require.NoError(t, err)
					}
					transport.phase(input.ID, "measure")
					follower = qualificationWireClient(t, ctx, base, output, user+"-follower", user, manifest, httpClient)
					defer follower.Close()
					_, err = follower.PullToStable(ctx)
					require.NoError(t, err)
					expected := input.Rows
					if input.ID == "small-single-row-delete" {
						expected = nil
					}
					qualificationWireAssertState(t, ctx, follower.Database(), manifest.Generation, expected)
				}
			}
			transport.phase(input.ID, "receipt")
			transport.lock.Lock()
			var selected []qualificationWireExchange
			for _, row := range transport.exchanges {
				if row.Workload == input.ID {
					require.Equal(t, envelope.Nonce, row.Nonce)
					selected = append(selected, row)
				}
			}
			transport.lock.Unlock()
			require.NotEmpty(t, selected)
			qualificationWireVerifyProtocol(t, manifest.Generation, selected)
			evidence := map[string]any{"workload": input.ID, "nonce": envelope.Nonce, "runtime": "raw", "expected_rows": input.ExpectedRows, "exchanges": selected, "full_state_verified": true}
			encoded, err := json.Marshal(evidence)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(output, fmt.Sprintf("raw-%02d.json", index)), encoded, 0600))
			receipt, err := json.Marshal(map[string]any{"nonce": envelope.Nonce, "runtime": "raw", "workload": input.ID, "evidence": json.RawMessage(encoded)})
			require.NoError(t, err)
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/qualification/wire/receipt", bytes.NewReader(receipt))
			require.NoError(t, err)
			request.Header.Set("Content-Type", "application/json")
			response, err := httpClient.Do(request)
			require.NoError(t, err)
			require.Equal(t, 204, response.StatusCode)
			require.NoError(t, response.Body.Close())
			t.Logf("QUALIFICATION_WIRE_WORKLOAD_COMPLETE runtime=raw nonce=%s workload=%s rows=%d", envelope.Nonce, input.ID, input.ExpectedRows)
		})
	}
}

func qualificationWireClient(t *testing.T, ctx context.Context, base, output, name, user string, manifest qualificationWireManifest, httpClient *http.Client) *oversqlite.Client {
	t.Helper()
	var tables []oversqlite.SyncTable
	for _, table := range manifest.Generation.Tables {
		tables = append(tables, oversqlite.SyncTable{TableName: table.Table, SyncKeyColumns: table.SyncKeyColumns})
	}
	config := defaultConfigForTest(t, "business", tables)
	config.DatabaseID = "qualification-wire"
	token := func(ctx context.Context) (string, error) {
		body, _ := json.Marshal(map[string]string{"user": user, "password": "synthetic"})
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/dummy-signin", bytes.NewReader(body))
		if err != nil {
			return "", err
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := httpClient.Do(request)
		if err != nil {
			return "", err
		}
		defer response.Body.Close()
		if response.StatusCode != 200 {
			return "", fmt.Errorf("signin status%d", response.StatusCode)
		}
		var value struct{ Token string }
		err = json.NewDecoder(response.Body).Decode(&value)
		return value.Token, err
	}
	plan := &oversqlite.UpgradePlan{PlanID: "qualification-wire-fresh-v1", Mode: oversqlite.UpgradeModeFresh, DatabaseID: config.DatabaseID, ServiceID: config.ServiceID, TargetGenerationID: 1, TargetVersion: 1, Migrator: scopeFreshMigrator{ddl: manifest.DDL}}
	opening, err := oversqlite.NewOpening(&oversqlite.OpeningConfig{DatabasePath: filepath.Join(output, name+".sqlite"), Plan: plan, BaseURL: base, Token: token, HTTP: httpClient, TargetClientConfig: config})
	require.NoError(t, err)
	result, err := opening.Advance(ctx)
	require.NoError(t, err)
	require.NotNil(t, result.Ready)
	_, err = result.Ready.Attach(ctx, user)
	require.NoError(t, err)
	return result.Ready
}

func qualificationWireApply(t *testing.T, ctx context.Context, db *sql.DB, rows []oversync.PushRequestRow) {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	for _, row := range rows {
		if row.Op == oversync.OpDelete {
			_, err = tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %q WHERE id=?", row.Table), row.Key["id"])
			require.NoError(t, err)
			continue
		}
		var payload map[string]any
		require.NoError(t, json.Unmarshal(row.Payload, &payload))
		var keys []string
		for key := range payload {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		var columns, marks []string
		var values []any
		for _, key := range keys {
			columns = append(columns, fmt.Sprintf("%q", key))
			marks = append(marks, "?")
			values = append(values, payload[key])
		}
		_, err = tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %q (%s) VALUES (%s)", row.Table, strings.Join(columns, ","), strings.Join(marks, ",")), values...)
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
}

func qualificationWireAssertState(t *testing.T, ctx context.Context, db *sql.DB, generation oversync.SchemaGenerationCapabilities, expected []oversync.PushRequestRow) {
	t.Helper()
	byTable := map[string][]map[string]any{}
	for _, row := range expected {
		var payload map[string]any
		require.NoError(t, json.Unmarshal(row.Payload, &payload))
		byTable[row.Table] = append(byTable[row.Table], payload)
	}
	for _, table := range generation.Tables {
		var columns []string
		for _, column := range table.Columns {
			columns = append(columns, fmt.Sprintf("%q", column.Name))
		}
		rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT %s FROM %q ORDER BY id", strings.Join(columns, ","), table.Table))
		require.NoError(t, err)
		var actual []map[string]any
		for rows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			require.NoError(t, rows.Scan(pointers...))
			row := map[string]any{}
			for i, column := range table.Columns {
				row[column.Name] = values[i]
			}
			actual = append(actual, row)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		want := byTable[table.Table]
		sort.Slice(want, func(i, j int) bool { return fmt.Sprint(want[i]["id"]) < fmt.Sprint(want[j]["id"]) })
		require.Equal(t, want, actual, "complete typed business state for%s", table.Table)
	}
	var foreignKeys int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM pragma_foreign_key_check").Scan(&foreignKeys))
	require.Zero(t, foreignKeys)
}

func qualificationWireVerifyProtocol(t *testing.T, generation oversync.SchemaGenerationCapabilities, exchanges []qualificationWireExchange) {
	t.Helper()
	for _, exchange := range exchanges {
		if exchange.Route == "/sync/push" {
			var request oversync.CompactPushRequest
			require.NoError(t, json.Unmarshal(exchange.Request, &request))
			rows, err := oversync.ExpandCompactPushRows(generation, request.Rows)
			require.NoError(t, err)
			registry, err := oversync.CompileSchemaGenerationRegistry(RegisteredTablesForBusinessSchema("business"), SchemaGenerationsForBusinessSchema("business"))
			require.NoError(t, err)
			contract, ok := registry.Generation(1)
			require.True(t, ok)
			encoded, err := oversync.QualificationWirePush(contract, request.SourceBundleID, rows)
			require.NoError(t, err)
			var canonical oversync.CompactPushRequest
			require.NoError(t, json.Unmarshal(encoded, &canonical))
			require.Equal(t, canonical.CanonicalRequestHash, request.CanonicalRequestHash)
		}
		if exchange.Route == "/sync/pull" {
			var response oversync.CompactPullResponse
			require.NoError(t, json.Unmarshal(exchange.Response, &response))
			for _, bundle := range response.Bundles {
				expanded, err := oversync.ExpandCompactPushRows(generation, bundle.Rows)
				require.NoError(t, err)
				rows := make([]oversync.BundleRow, 0, len(expanded))
				for _, row := range expanded {
					rows = append(rows, oversync.BundleRow{Schema: row.Schema, Table: row.Table, Key: row.Key, Op: row.Op, RowVersion: row.BaseRowVersion, Payload: row.Payload})
				}
				hash, _, err := oversync.QualificationWireBundleHash(rows)
				require.NoError(t, err)
				require.Equal(t, bundle.BundleHash, hash)
			}
		}
	}
}
