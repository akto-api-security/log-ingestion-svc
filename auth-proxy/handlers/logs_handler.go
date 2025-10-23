package handlers

import (
	"encoding/json"
	"io"
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
	log.Printf("Authenticated request from account: %s", accountID)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Failed to read request body: %v", err)
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var logs []map[string]interface{}
	if err := json.Unmarshal(body, &logs); err != nil {
		log.Printf("Failed to parse logs: %v", err)
		http.Error(w, "Bad request: invalid JSON", http.StatusBadRequest)
		return
	}

	if err := h.storage.StoreLogs(r.Context(), accountID, logs); err != nil {
		log.Printf("Failed to store logs: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"success"}`))
}
