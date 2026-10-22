// Qualification-only facade. Overlay this new file inside package oversync;
// production encoders/compressors and their existing source are unchanged.
package oversync

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"net/http"
	"net/http/httptest"
)

// QualificationWireIncompressibleBody preserves deterministicCorpusBody's
// exact4096-byte xorshift64star case, including its original fill offsets.
// The result is opaque compression-selector input, not a sync JSON message.
func QualificationWireIncompressibleBody() []byte {
	const size = 4096
	body := make([]byte, size)
	copy(body, `{"pad":"`)
	copy(body[size-2:], `"}`)
	state := uint64(0x6f76657273716c69)
	for index := 8; index < size-2; index++ {
		state ^= state >> 12
		state ^= state << 25
		state ^= state >> 27
		body[index] = byte((state * 2685821657736338717) >> 56)
	}
	return body
}

// QualificationWriteWireIncompressible reaches the current production gzip
// selector. This facade performs no independent compression or JSON rewriting.
func QualificationWriteWireIncompressible(w http.ResponseWriter, r *http.Request) {
	(&HTTPSyncHandlers{}).writeJSONBytes(w, r, http.StatusOK, QualificationWireIncompressibleBody())
}

// QualificationWireGeneration exposes the current immutable production descriptor.
func QualificationWireGeneration(contract SchemaGenerationContract) SchemaGenerationCapabilities {
	return generationDescriptor(contract)
}

// QualificationWirePush uses the production generation-pinned hash and compact encoder.
func QualificationWirePush(contract SchemaGenerationContract, sourceBundleID int64, rows []PushRequestRow) ([]byte, error) {
	hash, err := computeGenerationPinnedPushRequestHash(SchemaGenerationProof{ID: contract.ID(), Fingerprint: contract.Fingerprint()}, rows)
	if err != nil {
		return nil, err
	}
	groups, err := CompactGroupsForRows(generationDescriptor(contract), rows)
	if err != nil {
		return nil, err
	}
	return json.Marshal(CompactPushRequest{SourceBundleID: sourceBundleID, CanonicalRequestHash: hash, Rows: groups})
}

// QualificationWireBundleHash uses exactly the committed production hash domain.
func QualificationWireBundleHash(rows []BundleRow) (string, int64, error) {
	hash, bytes, err := computeCommittedBundleHash(rows)
	if err != nil {
		return "", 0, err
	}
	return renderBundleHash(hash), bytes, nil
}

// QualificationWireVariants retains a comparison gzip stream from the same Go
// compressor as writeJSONBytes and separately calls the production selector.
// Only the selector output is eligible to equal an actual server response.
func QualificationWireVariants(body []byte) (selected, comparison []byte, err error) {
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	if _, err = writer.Write(body); err != nil {
		return nil, nil, err
	}
	if err = writer.Close(); err != nil {
		return nil, nil, err
	}
	request := httptest.NewRequest(http.MethodGet, "http://qualification.invalid/", nil)
	request.Header.Set("Accept-Encoding", "gzip")
	recorder := httptest.NewRecorder()
	(&HTTPSyncHandlers{}).writeJSONBytes(recorder, request, http.StatusOK, body)
	return append([]byte(nil), recorder.Body.Bytes()...), append([]byte(nil), buffer.Bytes()...), nil
}
