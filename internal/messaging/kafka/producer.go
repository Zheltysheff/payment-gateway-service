package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	payments "payment-gateway-service/internal/pb/payments"
	"payment-gateway-service/internal/service/api"
)

type Producer struct {
	writer       *kafka.Writer
	commandTopic string
}

func NewProducer(brokers []string, commandTopic string) *Producer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		BatchTimeout: 10 * time.Millisecond,
	}
	return &Producer{writer: w, commandTopic: commandTopic}
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

func (p *Producer) PublishCreatePayment(ctx context.Context, cmd api.CreatePaymentCommand) error {
	pbCmd := &payments.CreatePaymentCommand{
		PaymentId:  cmd.PaymentID.String(),
		Amount:     cmd.Amount,
		Currency:   cmd.Currency,
		MerchantId: cmd.MerchantID,
		OrderId:    cmd.OrderID,
		UserId:     cmd.UserID,
		IssuedAt:   cmd.IssuedAt.UnixNano(),
	}

	value, err := proto.Marshal(pbCmd)
	if err != nil {
		return fmt.Errorf("Marshal CreatePaymentCommand: %w", err)
	}

	msg := kafka.Message{
		Topic: p.commandTopic,
		Key:   []byte(cmd.PaymentID.String()),
		Value: value,
		Headers: []kafka.Header{
			{Key: "command_type", Value: []byte("CreatePayment")},
			{Key: "content_type", Value: []byte("application/x-protobuf")},
		},
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("Write kafka message: %w", err)
	}
	return nil
}
