package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"payment-gateway-service/internal/config"
	"syscall"
	"time"

	"github.com/google/uuid"
)

type createPaymentRequest struct {
	PaymentID  string `json:"payment_id"`
	Amount     int64  `json:"amount"`
	Currency   string `json:"currency"`
	MerchantID string `json:"merchant_id"`
	OrderID    string `json:"order_id"`
	UserID     string `json:"user_id"`
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

	client := &http.Client{Timeout: 5 * time.Second}

	for {
		interval := time.Duration(rand.IntN(3000)+500) * time.Millisecond

		select {
		case <-rootCtx.Done():
			logger.Info("client shutdown")
			return nil
		case <-time.After(interval):
		}

		req := createPaymentRequest{
			PaymentID:  uuid.New().String(),
			Amount:     int64(rand.IntN(9000) + 1000),
			Currency:   "USD",
			MerchantID: "Stripe",
			OrderID:    uuid.New().String(),
			UserID:     "user_test",
		}

		body, err := json.Marshal(req)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}

		logger.Info("create request", "payment_id", req.PaymentID)

		httpReq, err := http.NewRequestWithContext(rootCtx, http.MethodPost, cfg.Client.Url+"/api/payments",
			bytes.NewReader(body))
		if err != nil {
			return err
		}
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(httpReq)
		if err != nil {
			logger.Error("post request has error", "error", err)
			continue
		}
		if resp.StatusCode >= 400 {
			logger.Warn("non positive status code", "status", resp.StatusCode)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
}
