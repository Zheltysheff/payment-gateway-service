package projection

import (
	"context"
	"fmt"
	"payment-gateway-service/internal/domain"
	"time"

	"github.com/google/uuid"
)

type PaymentRepository interface {
	Insert(ctx context.Context, p domain.Payment) error
	UpdateStatus(ctx context.Context, id uuid.UUID, status domain.Status, updatedAt time.Time) error
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

	if err := s.repo.Insert(ctx, payment); err != nil {
		return fmt.Errorf("project payment created %s: %w", ev.PaymentID, err)
	}
	return nil
}

func (s *PaymentService) HandlePaymentCompleted(ctx context.Context, ev *domain.PaymentCompletedEvent) error {
	if err := s.repo.UpdateStatus(ctx, ev.PaymentID, domain.StatusCompleted, ev.OccurredAt); err != nil {
		return fmt.Errorf("project payment completed %s: %w", ev.PaymentID, err)
	}
	return nil
}

func (s *PaymentService) HandlePaymentFailed(ctx context.Context, ev *domain.PaymentFailedEvent) error {
	if err := s.repo.UpdateStatus(ctx, ev.PaymentID, domain.StatusFailed, ev.OccurredAt); err != nil {
		return fmt.Errorf("project payment failed %s: %w", ev.PaymentID, err)
	}
	return nil
}
