package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"payment-gateway-service/internal/domain"
	payments "payment-gateway-service/internal/pb/payments"
	"payment-gateway-service/internal/service/projection"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

type EventConsumerHandler struct {
	service *projection.PaymentService
	logger  *slog.Logger
}

func NewEventConsumerHandler(service *projection.PaymentService, logger *slog.Logger) *EventConsumerHandler {
	return &EventConsumerHandler{service: service, logger: logger}
}

func (h *EventConsumerHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *EventConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *EventConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if err := h.handleMessage(session.Context(), msg); err != nil {
			h.logger.Error("handle message failed", "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "error", err)
			session.MarkMessage(msg, "")
			continue
		}
		session.MarkMessage(msg, "")
	}
	return nil
}

func (h *EventConsumerHandler) handleMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	ctx = otel.GetTextMapPropagator().Extract(ctx, &consumerHeadersCarrier{headers: msg.Headers})

	tracer := otel.Tracer("kafka.consumer")
	ctx, span := tracer.Start(ctx, "consume "+msg.Topic,
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	defer span.End()

	var eventType string
	for _, header := range msg.Headers {
		if string(header.Key) == "event_type" {
			eventType = string(header.Value)
			break
		}
	}
	if eventType == "" {
		return fmt.Errorf("missing event_type header")
	}

	switch eventType {
	case domain.EventTypePaymentCreated:
		var pbPce payments.PaymentCreatedEvent
		if err := proto.Unmarshal(msg.Value, &pbPce); err != nil {
			return fmt.Errorf("unmarshal proto: %w", err)
		}

		commandID, err := uuid.Parse(pbPce.GetCommandId())
		if err != nil {
			return fmt.Errorf("parse command_id: %w", err)
		}
		paymentID, err := uuid.Parse(pbPce.GetPaymentId())
		if err != nil {
			return fmt.Errorf("parse payment_id: %w", err)
		}

		event := domain.PaymentCreatedEvent{
			PaymentID:  paymentID,
			CommandID:  commandID,
			Amount:     pbPce.GetAmount(),
			Currency:   pbPce.GetCurrency(),
			MerchantID: pbPce.GetMerchantId(),
			OrderID:    pbPce.GetOrderId(),
			UserID:     pbPce.GetUserId(),
			OccurredAt: time.Unix(0, pbPce.GetOccurredAt()).UTC(),
		}

		if err := h.service.HandlePaymentCreated(ctx, &event); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "handle failed")
			return fmt.Errorf("handle create payment %s with event %s: %w", paymentID, commandID, err)
		}

		return nil
	case domain.EventTypePaymentCompleted:
		var pbPce payments.PaymentCompletedEvent
		if err := proto.Unmarshal(msg.Value, &pbPce); err != nil {
			return fmt.Errorf("unmarshal proto: %w", err)
		}

		commandID, err := uuid.Parse(pbPce.GetCommandId())
		if err != nil {
			return fmt.Errorf("parse command_id: %w", err)
		}
		paymentID, err := uuid.Parse(pbPce.GetPaymentId())
		if err != nil {
			return fmt.Errorf("parse payment_id: %w", err)
		}

		event := domain.PaymentCompletedEvent{
			PaymentID:  paymentID,
			CommandID:  commandID,
			OccurredAt: time.Unix(0, pbPce.GetOccurredAt()).UTC(),
		}

		if err := h.service.HandlePaymentCompleted(ctx, &event); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "handle failed")
			return fmt.Errorf("handle completed payment %s with event %s: %w", paymentID, commandID, err)
		}

		return nil
	case domain.EventTypePaymentFailed:
		var pbPfe payments.PaymentFailedEvent
		if err := proto.Unmarshal(msg.Value, &pbPfe); err != nil {
			return fmt.Errorf("unmarshal proto: %w", err)
		}

		commandID, err := uuid.Parse(pbPfe.GetCommandId())
		if err != nil {
			return fmt.Errorf("parse command_id: %w", err)
		}
		paymentID, err := uuid.Parse(pbPfe.GetPaymentId())
		if err != nil {
			return fmt.Errorf("parse payment_id: %w", err)
		}

		event := domain.PaymentFailedEvent{
			PaymentID:  paymentID,
			CommandID:  commandID,
			Reason:     pbPfe.GetReason(),
			OccurredAt: time.Unix(0, pbPfe.GetOccurredAt()).UTC(),
		}

		if err := h.service.HandlePaymentFailed(ctx, &event); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "handle failed")
			return fmt.Errorf("handle failed payment %s with event %s: %w", paymentID, commandID, err)
		}

		return nil
	default:
		h.logger.Warn("unknown event type", "type", eventType)
		return nil
	}
}
