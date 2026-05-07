package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"payment-gateway-service/internal/config"
	"payment-gateway-service/internal/messaging/kafka"
	"payment-gateway-service/internal/observability"
	"payment-gateway-service/internal/repository/postgres"
	"payment-gateway-service/internal/service/worker"
	"sync"
	"syscall"
	"time"
)

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

	logger.Info("worker starting", "service", cfg.Service.Worker.Name)

	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := observability.SetupTracing(rootCtx, cfg.Service.Worker.Name, cfg.Observability.OTLP.Endpoint)
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

	pool, err := postgres.NewPool(rootCtx, cfg.Postgres)
	if err != nil {
		return err
	}
	defer pool.Close()

	eventStoreRepo := postgres.NewEventStoreRepository(pool)

	consumer, err := kafka.NewConsumer(cfg.Kafka.Brokers, cfg.Kafka.GroupID)
	if err != nil {
		return err
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			logger.Warn("close kafka consumer", "error", err)
		}
	}()

	paymentService := worker.New(eventStoreRepo)
	consumerHandler := kafka.NewConsumerHandler(paymentService, logger)

	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			if err := consumer.Consume(rootCtx, []string{cfg.Kafka.Topics.Commands}, consumerHandler); err != nil {
				logger.Error("consume session", "error", err)
			}
			if rootCtx.Err() != nil {
				return
			}
		}
	})

	<-rootCtx.Done()
	logger.Info("shutdown signal received")
	wg.Wait()

	return nil
}
