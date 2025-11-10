package server

import (
	"log"
	"net/http"
	"time"

	"auth-proxy/auth"
	"auth-proxy/config"
	"auth-proxy/handlers"
	"auth-proxy/middleware"
	"auth-proxy/storage"
)

type Server struct {
	config    *config.Config
	validator auth.Validator
	storage   storage.LogStorage
}

func New(cfg *config.Config, validator auth.Validator, storage storage.LogStorage) *Server {
	return &Server{
		config:    cfg,
		validator: validator,
		storage:   storage,
	}
}

func (s *Server) Start() error {
	mux := http.NewServeMux()

	logsHandler := handlers.NewLogsHandler(s.storage)
	authMiddleware := middleware.AuthMiddleware(s.validator)
	mux.Handle("/logs", authMiddleware(logsHandler))

	healthHandler := handlers.NewHealthHandler()
	mux.Handle("/health", healthHandler)

	handler := middleware.LoggingMiddleware(mux)

	httpServer := &http.Server{
		Addr:         ":" + s.config.Port,
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Printf("Starting auth proxy on port %s", s.config.Port)
	return httpServer.ListenAndServe()
}
