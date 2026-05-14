package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"

	"payment-gateway-service/internal/config"
	"payment-gateway-service/internal/messaging/kafka"
	"payment-gateway-service/internal/observability"
	"payment-gateway-service/internal/service/callback"
)

type webhookRequest struct {
	PaymentID string `json:"payment_id"`
	Status    string `json:"status"`
	Reason    string `json:"reason,omitempty"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	configPath := flag.String("config", "/etc/payment-gateway/config.yaml", "path to config file")
	flag.Parse()

	if err := run(logger, *configPath); err != nil {
		logger.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger, configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}

	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := observability.SetupTracing(rootCtx, cfg.Service.Callback.Name, cfg.Observability.OTLP.Endpoint)
	if err != nil {
		return err
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTracing(ctx); err != nil {
			logger.Warn("shutdown tracing", "error", err)
		}
	}()

	producer, err := kafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.Topics.Commands, cfg.Kafka.Topics.Events)
	if err != nil {
		return err
	}
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Warn("close kafka producer", "error", err)
		}
	}()

	callbackService := callback.New(producer)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhooks/psp", webhookHandler(callbackService, logger))

	server := &http.Server{
		Addr:              net.JoinHostPort("", strconv.Itoa(cfg.Service.Callback.HTTP.Port)),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("callback HTTP server starting", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
		close(serverErr)
	}()

	select {
	case <-rootCtx.Done():
		logger.Info("shutdown signal received")
	case err := <-serverErr:
		if err != nil {
			return err
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Warn("HTTP server shutdown", "error", err)
	}
	return nil
}

func webhookHandler(svc *callback.PaymentService, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req webhookRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "Invalid json body")
			return
		}

		paymentID, err := uuid.Parse(req.PaymentID)
		if err != nil {
			writeError(w, http.StatusBadRequest, "Invalid payment_id")
			return
		}

		if req.Status != "completed" && req.Status != "failed" {
			writeError(w, http.StatusBadRequest, "Invalid status")
			return
		}

		logger.InfoContext(r.Context(), "psp webhook received", "payment_id", req.PaymentID, "status", req.Status)

		if err := svc.HandlePSPCallback(r.Context(), paymentID, req.Status, req.Reason); err != nil {
			logger.ErrorContext(r.Context(), "handle psp callback", "error", err, "payment_id", req.PaymentID)
			writeError(w, http.StatusInternalServerError, "Failed to process callback")
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}
