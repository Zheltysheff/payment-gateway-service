package callback

import (
	"context"
	"fmt"
	"payment-gateway-service/internal/domain"
	"payment-gateway-service/internal/pb/payments"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

type CommandPublisher interface {
	PublishCommand(ctx context.Context, paymentID uuid.UUID, commandType domain.CommandType, pbMsg proto.Message) error
}

type PaymentService struct {
	publisher CommandPublisher
}

func New(publisher CommandPublisher) *PaymentService {
	return &PaymentService{publisher: publisher}
}

func (s *PaymentService) HandlePSPCallback(ctx context.Context, paymentID uuid.UUID, status string, reason string) error {
	commandID := uuid.New()
	var pbCmd proto.Message
	var cmdType domain.CommandType

	switch status {
	case "completed":
		pbCmd = &payments.MarkCompletedCommand{
			CommandId: commandID.String(),
			PaymentId: paymentID.String(),
			IssuedAt:  time.Now().UTC().UnixNano(),
		}
		cmdType = domain.CmdMarkCompleted
	case "failed":
		pbCmd = &payments.MarkFailedCommand{
			CommandId: commandID.String(),
			PaymentId: paymentID.String(),
			Reason:    reason,
			IssuedAt:  time.Now().UTC().UnixNano(),
		}
		cmdType = domain.CmdMarkFailed
	default:
		return fmt.Errorf("unknown status %q", status)
	}

	return s.publisher.PublishCommand(ctx, paymentID, cmdType, pbCmd)
}
