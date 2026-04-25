package main

import (
	"context"
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

	"payment-gateway-service/internal/config"
	httpHandler "payment-gateway-service/internal/http"
	"payment-gateway-service/internal/messaging/kafka"
	"payment-gateway-service/internal/observability"
	"payment-gateway-service/internal/repository/postgres"
	paymentapi "payment-gateway-service/internal/service/api"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	configPath := flag.String("config", "/etc/payment-gateway/config.yaml", "path to config file")
	flag.Parse()

	if err := run(logger, *configPath); err != nil {
		logger.Error("Fatal", "error", err)
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

	shutdownTracing, err := observability.SetupTracing(rootCtx, cfg.Service.Name, cfg.Observability.OTLP.Endpoint)
	if err != nil {
		return err
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTracing(ctx); err != nil {
			logger.Warn("Shutdown tracing", "error", err)
		}
	}()

	registry := observability.NewRegistry()
	httpMetrics := observability.NewHTTPMetrics(registry)

	pool, err := postgres.NewPool(rootCtx, cfg.Postgres)
	if err != nil {
		return err
	}
	defer pool.Close()

	producer := kafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.Topics.Commands)
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Warn("Close kafka producer", "error", err)
		}
	}()

	repo := postgres.NewPaymentRepository(pool)
	paymentService := paymentapi.New(repo, producer)
	handler := httpHandler.NewHandler(paymentService, logger)
	router := httpHandler.NewRouter(handler, registry, httpMetrics)

	server := &http.Server{
		Addr:              net.JoinHostPort("", strconv.Itoa(cfg.Service.HTTP.Port)),
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("HTTP server working at ", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
		close(serverErr)
	}()

	select {
	case <-rootCtx.Done():
		logger.Info("Shutdown signal received")
	case err := <-serverErr:
		if err != nil {
			return err
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Warn("Http server shutdown", "error", err)
	}
	return nil
}
