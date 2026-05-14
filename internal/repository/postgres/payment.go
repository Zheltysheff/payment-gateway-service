package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"payment-gateway-service/internal/domain"
)

type PaymentRepository struct {
	pool *pgxpool.Pool
}

func NewPaymentRepository(pool *pgxpool.Pool) *PaymentRepository {
	return &PaymentRepository{pool}
}

const getByIdQuery = `
SELECT id, amount, currency, merchant_id, order_id, user_id, status, created_at, updated_at
FROM payments
WHERE id = $1
`

const insertQuery = `
INSERT INTO payments (id, amount, currency, merchant_id, order_id, user_id, status, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (id) DO NOTHING
`

const updateStatusQuery = `
UPDATE payments
SET status = $2, updated_at = $3
WHERE id = $1
`

func (r *PaymentRepository) GetByID(ctx context.Context, id uuid.UUID) (*domain.Payment, error) {
	var p domain.Payment
	err := r.pool.QueryRow(ctx, getByIdQuery, id).Scan(
		&p.ID,
		&p.Amount,
		&p.Currency,
		&p.MerchantID,
		&p.OrderID,
		&p.UserID,
		&p.Status,
		&p.CreatedAt,
		&p.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrPaymentNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("repo get payment %s: %w", id, err)
	}
	return &p, nil
}

func (r *PaymentRepository) Insert(ctx context.Context, p domain.Payment) error {
	if _, err := r.pool.Exec(ctx, insertQuery, p.ID, p.Amount, p.Currency, p.MerchantID, p.OrderID, p.UserID, string(p.Status), p.CreatedAt, p.UpdatedAt); err != nil {
		return fmt.Errorf("insert payment %s: %w", p.ID, err)
	}
	return nil
}

func (r *PaymentRepository) UpdateStatus(ctx context.Context, id uuid.UUID, status domain.Status, updatedAt time.Time) error {
	if _, err := r.pool.Exec(ctx, updateStatusQuery, id, string(status), updatedAt); err != nil {
		return fmt.Errorf("update payment %s status %s: %w", id, status, err)
	}
	return nil
}
