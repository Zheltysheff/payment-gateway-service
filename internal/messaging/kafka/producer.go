package kafka

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	payments "payment-gateway-service/internal/pb/payments"
	"payment-gateway-service/internal/service/api"
	"payment-gateway-service/internal/service/worker"
)

type Producer struct {
	producer     sarama.SyncProducer
	commandTopic string
}

func NewProducer(brokers []string, commandTopic string) (*Producer, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_8_0_0
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Partitioner = sarama.NewHashPartitioner
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true

	p, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("create sarama producer: %w", err)
	}
	return &Producer{producer: p, commandTopic: commandTopic}, nil
}

func (p *Producer) Close() error {
	return p.producer.Close()
}

func (p *Producer) PublishCreatePayment(ctx context.Context, cmd api.CreatePaymentCommand) error {
	tracer := otel.Tracer("kafka.producer")
	ctx, span := tracer.Start(ctx, "publish "+p.commandTopic,
		trace.WithSpanKind(trace.SpanKindProducer),
	)
	defer span.End()
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

	value, err := proto.Marshal(pbCmd)
	if err != nil {
		return fmt.Errorf("Marshal CreatePaymentCommand: %w", err)
	}

	headers := []sarama.RecordHeader{
		{Key: []byte("command_type"), Value: []byte(string(worker.CmdCreatePayment))},
		{Key: []byte("content_type"), Value: []byte("application/x-protobuf")},
	}

	otel.GetTextMapPropagator().Inject(ctx, &producerHeadersCarrier{headers: &headers})

	msg := &sarama.ProducerMessage{
		Topic:   p.commandTopic,
		Key:     sarama.StringEncoder(cmd.PaymentID.String()),
		Value:   sarama.ByteEncoder(value),
		Headers: headers,
	}

	type result struct {
		err error
	}
	done := make(chan result, 1)
	go func() {
		_, _, err := p.producer.SendMessage(msg)
		done <- result{err: err}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case r := <-done:
		if r.err != nil {
			span.RecordError(r.err)
			span.SetStatus(codes.Error, "send failed")
			return fmt.Errorf("send kafka message: %w", r.err)
		}
		return nil
	}
}
