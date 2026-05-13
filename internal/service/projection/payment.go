package projection

import (
	"context"
	"fmt"
	"payment-gateway-service/internal/domain"
)

type PaymentRepository interface {
	Upsert(ctx context.Context, p domain.Payment) error
}

type PaymentService struct {
	repo PaymentRepository
}

func New(repo PaymentRepository) *PaymentService {
	return &PaymentService{repo: repo}
}

func (s *PaymentService) HandlePaymentCreated(ctx context.Context, ev *domain.PaymentCreatedEvent) error {
	payment := domain.Payment{
		ID:         ev.PaymentID,
		Amount:     ev.Amount,
		Currency:   ev.Currency,
		MerchantID: ev.MerchantID,
		OrderID:    ev.OrderID,
		UserID:     ev.UserID,
		Status:     domain.StatusNew,
		CreatedAt:  ev.OccurredAt,
		UpdatedAt:  ev.OccurredAt,
	}

	if err := s.repo.Upsert(ctx, payment); err != nil {
		return fmt.Errorf("project payment created %s: %w", ev.PaymentID, err)
	}
	return nil
}
