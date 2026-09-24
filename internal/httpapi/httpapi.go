// Package httpapi exposes the calibration service over HTTP.
//
//	GET  /health
//	POST /v1/equipments/{equipment}/calibrations          (base batch calibration)
//	POST /v1/equipments/{equipment}/temperature-zones     (batch x temperature overlay)
//	GET  /v1/equipments/{equipment}/calibration?batch=N[&temperature=T][&revision=R]
//
// Errors are reported as {"error":{"code","message"}} with stable codes.
package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/example/calibsvc/internal/canonjson"
	"github.com/example/calibsvc/internal/store"
)

// Server routes HTTP requests to the store.
type Server struct {
	st  *store.Store
	mux *http.ServeMux
}

// New builds a Server with all routes registered.
func New(st *store.Store) *Server {
	s := &Server{st: st, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.HandleFunc("POST /v1/equipments/{equipment}/calibrations", s.handlePublish)
	s.mux.HandleFunc("POST /v1/equipments/{equipment}/temperature-zones", s.handlePublishZone)
	s.mux.HandleFunc("GET /v1/equipments/{equipment}/calibration", s.handleQuery)
	return s
}

// Handler returns the root HTTP handler.
func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.st.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "UNHEALTHY", "database unreachable")
		return
	}
	writeJSON(w, http.StatusOK, []byte(`{"status":"ok"}`))
}

type publishRequest struct {
	OperationID  *string         `json:"operation_id"`
	SeenRevision *int64          `json:"seen_revision"`
	Lower        *int64          `json:"lower"`
	Upper        *int64          `json:"upper"`
	Content      json.RawMessage `json:"content"`
}

type zonePublishRequest struct {
	OperationID  *string         `json:"operation_id"`
	SeenRevision *int64          `json:"seen_revision"`
	BatchLower   *int64          `json:"batch_lower"`
	BatchUpper   *int64          `json:"batch_upper"`
	TempLower    *int64          `json:"temp_lower"`
	TempUpper    *int64          `json:"temp_upper"`
	Content      json.RawMessage `json:"content"`
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	equipment := r.PathValue("equipment")

	var req publishRequest
	if !decodePublishRequest(w, r, &req) {
		return
	}

	if req.OperationID == nil || *req.OperationID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_PARAMETER", "operation_id is required")
		return
	}
	if req.SeenRevision == nil {
		writeError(w, http.StatusBadRequest, "INVALID_PARAMETER", "seen_revision is required")
		return
	}
	if *req.SeenRevision < 0 {
		writeError(w, http.StatusBadRequest, "INVALID_PARAMETER", "seen_revision must be >= 0")
		return
	}
	if req.Lower == nil {
		writeError(w, http.StatusBadRequest, "INVALID_PARAMETER", "lower is required")
		return
	}
	if req.Upper == nil {
		writeError(w, http.StatusBadRequest, "INVALID_PARAMETER", "upper is required")
		return
	}
	if *req.Lower >= *req.Upper {
		writeError(w, http.StatusBadRequest, "INVALID_INTERVAL", "lower must be less than upper")
		return
	}
	if req.Content == nil {
		writeError(w, http.StatusBadRequest, "INVALID_PARAMETER", "content is required")
		return
	}
	content, err := canonjson.Normalize(req.Content)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_CONTENT", "content is not valid JSON: "+err.Error())
		return
	}

	res, err := s.st.Publish(r.Context(), store.PublishParams{
		Equipment:    equipment,
		OperationID:  *req.OperationID,
		SeenRevision: *req.SeenRevision,
		Lower:        *req.Lower,
		Upper:        *req.Upper,
		Content:      content,
		RequestHash: baseRequestHash(equipment, *req.OperationID, *req.SeenRevision,
			*req.Lower, *req.Upper, content),
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}

	writePublishResult(w, res)
}

func (s *Server) handlePublishZone(w http.ResponseWriter, r *http.Request) {
	equipment := r.PathValue("equipment")

	var req zonePublishRequest
	if !decodePublishRequest(w, r, &req) {
		return
	}

	if req.OperationID == nil || *req.OperationID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_PARAMETER", "operation_id is required")
		return
	}
	if req.SeenRevision == nil {
		writeError(w, http.StatusBadRequest, "INVALID_PARAMETER", "seen_revision is required")
		return
	}
	if *req.SeenRevision < 0 {
		writeError(w, http.StatusBadRequest, "INVALID_PARAMETER", "seen_revision must be >= 0")
		return
	}
	if req.BatchLower == nil {
		writeError(w, http.StatusBadRequest, "INVALID_PARAMETER", "batch_lower is required")
		return
	}
	if req.BatchUpper == nil {
		writeError(w, http.StatusBadRequest, "INVALID_PARAMETER", "batch_upper is required")
		return
	}
	if req.TempLower == nil {
		writeError(w, http.StatusBadRequest, "INVALID_PARAMETER", "temp_lower is required")
		return
	}
	if req.TempUpper == nil {
		writeError(w, http.StatusBadRequest, "INVALID_PARAMETER", "temp_upper is required")
		return
	}
	if *req.BatchLower >= *req.BatchUpper {
		writeError(w, http.StatusBadRequest, "INVALID_INTERVAL", "batch_lower must be less than batch_upper")
		return
	}
	if *req.TempLower >= *req.TempUpper {
		writeError(w, http.StatusBadRequest, "INVALID_INTERVAL", "temp_lower must be less than temp_upper")
		return
	}
	if req.Content == nil {
		writeError(w, http.StatusBadRequest, "INVALID_PARAMETER", "content is required")
		return
	}
	content, err := canonjson.Normalize(req.Content)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_CONTENT", "content is not valid JSON: "+err.Error())
		return
	}

	res, err := s.st.PublishZone(r.Context(), store.ZonePublishParams{
		Equipment:    equipment,
		OperationID:  *req.OperationID,
		SeenRevision: *req.SeenRevision,
		BatchLower:   *req.BatchLower,
		BatchUpper:   *req.BatchUpper,
		TempLower:    *req.TempLower,
		TempUpper:    *req.TempUpper,
		Content:      content,
		RequestHash: zoneRequestHash(equipment, *req.OperationID, *req.SeenRevision,
			*req.BatchLower, *req.BatchUpper, *req.TempLower, *req.TempUpper, content),
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}

	writePublishResult(w, res)
}

// decodePublishRequest decodes a single JSON document into v, rejecting
// malformed JSON, unknown fields and trailing data. It reports the error
// itself and returns false when the caller must stop handling.
func decodePublishRequest(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_JSON", "request body is not valid JSON: "+err.Error())
		return false
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "INVALID_JSON", "request body must contain a single JSON document")
		return false
	}
	return true
}

func writePublishResult(w http.ResponseWriter, res *store.PublishResult) {
	w.Header().Set("Content-Type", "application/json")
	if res.Replayed {
		w.Header().Set("X-Idempotent-Replay", "true")
	}
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write(res.Response)
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	equipment := r.PathValue("equipment")
	q := r.URL.Query()

	batchRaw := q.Get("batch")
	if batchRaw == "" {
		writeError(w, http.StatusBadRequest, "INVALID_PARAMETER", "batch query parameter is required")
		return
	}
	batch, err := strconv.ParseInt(batchRaw, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_PARAMETER", "batch must be an integer")
		return
	}

	var temperature *int64
	if tempRaw := q.Get("temperature"); tempRaw != "" {
		temp, err := strconv.ParseInt(tempRaw, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_PARAMETER", "temperature must be an integer")
			return
		}
		temperature = &temp
	}

	var revision *int64
	if revRaw := q.Get("revision"); revRaw != "" {
		rev, err := strconv.ParseInt(revRaw, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_PARAMETER", "revision must be an integer")
			return
		}
		if rev < 1 {
			writeError(w, http.StatusBadRequest, "INVALID_PARAMETER", "revision must be >= 1")
			return
		}
		revision = &rev
	}

	res, err := s.st.Query(r.Context(), equipment, &batch, temperature, revision)
	if err != nil {
		writeStoreError(w, err)
		return
	}

	body, _ := json.Marshal(struct {
		Equipment   string          `json:"equipment"`
		Batch       int64           `json:"batch"`
		Temperature *int64          `json:"temperature,omitempty"`
		Revision    int64           `json:"revision"`
		Source      string          `json:"source,omitempty"`
		Lower       int64           `json:"lower"`
		Upper       int64           `json:"upper"`
		TempLower   *int64          `json:"temp_lower,omitempty"`
		TempUpper   *int64          `json:"temp_upper,omitempty"`
		Content     json.RawMessage `json:"content"`
	}{
		Equipment:   res.Equipment,
		Batch:       res.Batch,
		Temperature: res.Temperature,
		Revision:    res.Revision,
		Source:      twoDSource(res),
		Lower:       res.Lower,
		Upper:       res.Upper,
		TempLower:   zoneBound(res, false),
		TempUpper:   zoneBound(res, true),
		Content:     json.RawMessage(res.Content),
	})
	writeJSON(w, http.StatusOK, body)
}

// twoDSource reports the effective layer for a two-dimensional query; it
// stays empty for one-dimensional queries so their response shape is
// unchanged.
func twoDSource(r *store.QueryResult) string {
	if r.Temperature == nil {
		return ""
	}
	return r.Source
}

// zoneBound returns the temperature extent of a zone hit, or nil for a
// base-calibration hit (so one-dimensional responses keep their shape).
func zoneBound(r *store.QueryResult, upper bool) *int64 {
	if r.Source != "zone" {
		return nil
	}
	if upper {
		v := r.TempUpper
		return &v
	}
	v := r.TempLower
	return &v
}

// requestHash binds an operation id to its full parameter set so that
// reused ids with different parameters are detected deterministically.
// Base keeps its historical hash format exactly; zone publishes carry a
// distinct "zone" prefix, so reusing an operation id across the two entry
// points is a stable OPERATION_CONFLICT as well.
func baseRequestHash(equipment, operationID string, seenRevision, lower, upper int64, content string) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%d\x00%d\x00%d\x00%s",
		equipment, operationID, seenRevision, lower, upper, content)
	return hex.EncodeToString(h.Sum(nil))
}

func zoneRequestHash(equipment, operationID string, seenRevision, bl, bu, tl, tu int64, content string) string {
	h := sha256.New()
	fmt.Fprintf(h, "zone\x00%s\x00%s\x00%d\x00%d\x00%d\x00%d\x00%d\x00%s",
		equipment, operationID, seenRevision, bl, bu, tl, tu, content)
	return hex.EncodeToString(h.Sum(nil))
}

func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrStaleRevision):
		writeError(w, http.StatusConflict, "STALE_REVISION", err.Error())
	case errors.Is(err, store.ErrOperationConflict):
		writeError(w, http.StatusConflict, "OPERATION_CONFLICT", err.Error())
	case errors.Is(err, store.ErrEquipmentNotFound):
		writeError(w, http.StatusNotFound, "EQUIPMENT_NOT_FOUND", err.Error())
	case errors.Is(err, store.ErrRevisionNotFound):
		writeError(w, http.StatusNotFound, "REVISION_NOT_FOUND", err.Error())
	case errors.Is(err, store.ErrBatchNotCovered):
		writeError(w, http.StatusNotFound, "BATCH_NOT_COVERED", err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "INTERNAL", "internal error")
	}
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	var e struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	e.Error.Code = code
	e.Error.Message = message
	body, _ := json.Marshal(e)
	writeJSON(w, status, body)
}

func writeJSON(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
