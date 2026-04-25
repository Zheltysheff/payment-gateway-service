package api

import (
	"context"
	"time"

	"github.com/google/uuid"

	"payment-gateway-service/internal/domain"
)

type PaymentRepository interface {
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Payment, error)
}

type CommandPublisher interface {
	PublishCreatePayment(ctx context.Context, cmd CreatePaymentCommand) error
}

type CreatePaymentCommand struct {
	PaymentID  uuid.UUID
	Amount     int64
	Currency   string
	MerchantID string
	OrderID    string
	UserID     string
	IssuedAt   time.Time
}

type PaymentService struct {
	repo      PaymentRepository
	publisher CommandPublisher
}

func New(repo PaymentRepository, publisher CommandPublisher) *PaymentService {
	return &PaymentService{repo: repo, publisher: publisher}
}

func (s *PaymentService) CreatePayment(ctx context.Context, cmd CreatePaymentCommand) error {
	if cmd.IssuedAt.IsZero() {
		cmd.IssuedAt = time.Now().UTC()
	}
	return s.publisher.PublishCreatePayment(ctx, cmd)
}

func (s *PaymentService) GetPayment(ctx context.Context, id uuid.UUID) (*domain.Payment, error) {
	return s.repo.GetByID(ctx, id)
}
