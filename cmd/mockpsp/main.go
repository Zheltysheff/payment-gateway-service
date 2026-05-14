package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"
)

type paymentRequest struct {
	PaymentID   string `json:"payment_id"`
	Amount      int64  `json:"amount"`
	Currency    string `json:"currency"`
	CallbackURL string `json:"callback_url"`
}

type callbackPayload struct {
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

	if err := run(logger); err != nil {
		logger.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /payment", paymentHandler(logger))

	server := &http.Server{
		Addr:              net.JoinHostPort("", strconv.Itoa(8082)),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("mock PSP HTTP server starting", "addr", server.Addr)
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

func paymentHandler(logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req paymentRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "Invalid json body")
			return
		}

		_, err := uuid.Parse(req.PaymentID)
		if err != nil {
			writeError(w, http.StatusBadRequest, "Invalid payment_id")
			return
		}

		if req.CallbackURL == "" {
			writeError(w, http.StatusBadRequest, "Missing callback_url")
			return
		}

		logger.Info("payment request received", "payment_id", req.PaymentID, "amount", req.Amount)

		go computePSP(context.Background(), logger, req)

		w.WriteHeader(http.StatusAccepted)
	}
}

func computePSP(ctx context.Context, logger *slog.Logger, req paymentRequest) {
	time.Sleep(time.Duration(1+rand.Intn(3)) * time.Second)

	var status string
	var reason string

	if rand.Float64() < 0.8 {
		status = "completed"
	} else {
		status = "failed"
		reason = "insufficient_funds"
	}

	payload := callbackPayload{
		PaymentID: req.PaymentID,
		Status:    status,
		Reason:    reason,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		logger.Error("marshal request", "error", err)
		return
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", req.CallbackURL, bytes.NewReader(body))
	if err != nil {
		logger.Error("request", "error", err)
		return
	}

	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		logger.Error("response", "error", err)
		return
	}

	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		logger.Error("response status", "status", resp.Status)
		return
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
