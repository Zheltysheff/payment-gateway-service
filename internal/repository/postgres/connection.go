package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"

	"payment-gateway-service/internal/config"
)

func NewPool(ctx context.Context, cfg config.PostgresConfig) (*pgxpool.Pool, error) {
	pgCfg, err := pgxpool.ParseConfig(cfg.Url)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}

	pgCfg.ConnConfig.Tracer = otelpgx.NewTracer()
	if cfg.Pool.MaxConns > 0 {
		pgCfg.MaxConns = cfg.Pool.MaxConns
	}
	if cfg.Pool.MinConns > 0 {
		pgCfg.MinConns = cfg.Pool.MinConns
	}
	if cfg.Pool.MaxConnLifetime > 0 {
		pgCfg.MaxConnLifetime = cfg.Pool.MaxConnLifetime
	}

	pool, err := pgxpool.NewWithConfig(ctx, pgCfg)
	if err != nil {
		return nil, fmt.Errorf("Create postgres pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("Ping postgres: %w", err)
	}

	return pool, nil
}
