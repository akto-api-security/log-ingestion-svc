package handlers

import (
	"encoding/json"
	"log"
	"net/http"

	"auth-proxy/auth"
	"auth-proxy/middleware"
	"auth-proxy/storage"
)

type LogsHandler struct {
	storage storage.LogStorage
}

func NewLogsHandler(storage storage.LogStorage) *LogsHandler {
	return &LogsHandler{storage: storage}
}

func (h *LogsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	claims, ok := r.Context().Value(middleware.ClaimsContextKey).(*auth.Claims)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	accountID := claims.GetAccountID()

	var logs []map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&logs); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Async write - returns immediately
	if err := h.storage.StoreLogs(r.Context(), accountID, logs); err != nil {
		log.Printf("Failed to queue logs: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"status":"accepted"}`))
}
