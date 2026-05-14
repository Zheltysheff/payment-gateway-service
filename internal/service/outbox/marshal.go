package outbox

import (
	"encoding/json"
	"fmt"
	"payment-gateway-service/internal/domain"
	"payment-gateway-service/internal/pb/payments"

	"google.golang.org/protobuf/proto"
)

func eventToProto(eventType string, payloadJSON []byte) (proto.Message, error) {
	event, err := domain.NewEvent(eventType)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(payloadJSON, event); err != nil {
		return nil, fmt.Errorf("unmarshal payload json for %T event: %w", event, err)
	}

	switch e := event.(type) {
	case *domain.PaymentCreatedEvent:
		return &payments.PaymentCreatedEvent{
			PaymentId:  e.PaymentID.String(),
			CommandId:  e.CommandID.String(),
			Amount:     e.Amount,
			Currency:   e.Currency,
			MerchantId: e.MerchantID,
			OrderId:    e.OrderID,
			UserId:     e.UserID,
			OccurredAt: e.OccurredAt.UnixNano(),
		}, nil
	case *domain.PaymentCompletedEvent:
		return &payments.PaymentCompletedEvent{
			PaymentId:  e.PaymentID.String(),
			CommandId:  e.CommandID.String(),
			OccurredAt: e.OccurredAt.UnixNano(),
		}, nil
	case *domain.PaymentFailedEvent:
		return &payments.PaymentFailedEvent{
			PaymentId:  e.PaymentID.String(),
			CommandId:  e.CommandID.String(),
			Reason:     e.Reason,
			OccurredAt: e.OccurredAt.UnixNano(),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported event type %q", eventType)
	}
}
