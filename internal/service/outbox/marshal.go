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
		pbPce := &payments.PaymentCreatedEvent{
			PaymentId:  e.PaymentID.String(),
			CommandId:  e.CommandID.String(),
			Amount:     e.Amount,
			Currency:   e.Currency,
			MerchantId: e.MerchantID,
			OrderId:    e.OrderID,
			UserId:     e.UserID,
			OccurredAt: e.OccurredAt.UnixNano(),
		}

		return pbPce, nil
	default:
		return nil, fmt.Errorf("unsupported event type %q", eventType)
	}
}
