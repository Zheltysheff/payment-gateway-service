package api

import (
	"context"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"payment-gateway-service/internal/domain"
	"payment-gateway-service/internal/pb/payments"
)

type PaymentRepository interface {
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Payment, error)
}

type CommandPublisher interface {
	PublishCommand(ctx context.Context, paymentID uuid.UUID, commandType domain.CommandType, pbMsg proto.Message) error
}

type CreatePaymentCommand struct {
	PaymentID  uuid.UUID
	CommandID  uuid.UUID
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

	pbCmd := &payments.CreatePaymentCommand{
		PaymentId:  cmd.PaymentID.String(),
		CommandId:  cmd.CommandID.String(),
		Amount:     cmd.Amount,
		Currency:   cmd.Currency,
		MerchantId: cmd.MerchantID,
		OrderId:    cmd.OrderID,
		UserId:     cmd.UserID,
		IssuedAt:   cmd.IssuedAt.UnixNano(),
	}
	return s.publisher.PublishCommand(ctx, cmd.PaymentID, domain.CmdCreatePayment, pbCmd)
}

func (s *PaymentService) GetPayment(ctx context.Context, id uuid.UUID) (*domain.Payment, error) {
	return s.repo.GetByID(ctx, id)
}
