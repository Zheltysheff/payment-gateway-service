package postgres

import (
	"context"
	"errors"
	"fmt"

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

const getByIDQuery = `
SELECT id, amount, currency, merchant_id, order_id, user_id, status, created_at, updated_at
FROM payments
WHERE id = $1
`

func (r *PaymentRepository) GetByID(ctx context.Context, id uuid.UUID) (*domain.Payment, error) {
	var p domain.Payment
	err := r.pool.QueryRow(ctx, getByIDQuery, id).Scan(
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
		return nil, fmt.Errorf("Get payment %s: %w", id, err)
	}
	return &p, nil
}
