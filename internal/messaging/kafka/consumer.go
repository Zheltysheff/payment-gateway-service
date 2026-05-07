package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"payment-gateway-service/internal/domain"
	payments "payment-gateway-service/internal/pb/payments"
	"payment-gateway-service/internal/service/worker"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

type Consumer struct {
	consumer sarama.ConsumerGroup
	groupID  string
}

func NewConsumer(brokers []string, groupID string) (*Consumer, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_8_0_0
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Return.Errors = true

	c, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		return nil, fmt.Errorf("create sarama consumer: %w", err)
	}
	return &Consumer{consumer: c, groupID: groupID}, nil
}

func (c *Consumer) Close() error {
	return c.consumer.Close()
}

type ConsumerHandler struct {
	service *worker.PaymentService
	logger  *slog.Logger
}

func NewConsumerHandler(service *worker.PaymentService, logger *slog.Logger) *ConsumerHandler {
	return &ConsumerHandler{service: service, logger: logger}
}

func (h *ConsumerHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *ConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *ConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
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

func (c *Consumer) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	return c.consumer.Consume(ctx, topics, handler)
}

func (h *ConsumerHandler) handleMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	ctx = otel.GetTextMapPropagator().Extract(ctx, &consumerHeadersCarrier{headers: msg.Headers})

	tracer := otel.Tracer("kafka.consumer")
	ctx, span := tracer.Start(ctx, "consume "+msg.Topic,
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	defer span.End()

	var commandType string
	for _, header := range msg.Headers {
		if string(header.Key) == "command_type" {
			commandType = string(header.Value)
			break
		}
	}
	if commandType == "" {
		return fmt.Errorf("missing command_type header")
	}
	if worker.CommandType(commandType) != worker.CmdCreatePayment {
		return fmt.Errorf("unsupported command_type %q", commandType)
	}

	var pbCmd payments.CreatePaymentCommand
	if err := proto.Unmarshal(msg.Value, &pbCmd); err != nil {
		return fmt.Errorf("unmarshal proto: %w", err)
	}

	commandID, err := uuid.Parse(pbCmd.GetCommandId())
	if err != nil {
		return fmt.Errorf("parse command_id: %w", err)
	}
	paymentID, err := uuid.Parse(pbCmd.GetPaymentId())
	if err != nil {
		return fmt.Errorf("parse payment_id: %w", err)
	}

	cmd := domain.CreatePaymentCommand{
		CommandID:  commandID,
		PaymentID:  paymentID,
		Amount:     pbCmd.GetAmount(),
		Currency:   pbCmd.GetCurrency(),
		MerchantID: pbCmd.GetMerchantId(),
		OrderID:    pbCmd.GetOrderId(),
		UserID:     pbCmd.GetUserId(),
		IssuedAt:   time.Unix(0, pbCmd.GetIssuedAt()).UTC(),
	}

	if err := h.service.HandleCreatePayment(ctx, cmd); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "handle failed")
		return fmt.Errorf("handle create payment %s with command %s: %w", paymentID, commandID, err)
	}

	return nil
}
