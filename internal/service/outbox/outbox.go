package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

type UnpublishedEvent struct {
	ID         int64
	PaymentID  uuid.UUID
	EventType  string
	Payload    []byte
	OccurredAt time.Time
}

type EventStore interface {
	LoadUnpublishedEvents(ctx context.Context, limit int) ([]UnpublishedEvent, error)
	MarkEventsPublished(ctx context.Context, ids []int64) error
}

type Publisher interface {
	PublishEvent(ctx context.Context, paymentID uuid.UUID, eventType string, msg proto.Message) error
}

type Outbox struct {
	store     EventStore
	publisher Publisher
	logger    *slog.Logger
	batchSize int
	interval  time.Duration
}

func New(store EventStore, publisher Publisher, logger *slog.Logger, batchSize int, interval time.Duration) *Outbox {
	return &Outbox{store: store, publisher: publisher, logger: logger, batchSize: batchSize, interval: interval}
}

func (o *Outbox) Run(ctx context.Context) error {
	ticker := time.NewTicker(o.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := o.tick(ctx); err != nil {
				o.logger.Error("outbox tick failed", "error", err)
			}
		}
	}
}

func (o *Outbox) tick(ctx context.Context) error {
	events, err := o.store.LoadUnpublishedEvents(ctx, o.batchSize)
	if err != nil {
		return err
	}

	if len(events) == 0 {
		return nil
	}

	ids := make([]int64, 0, len(events))

	for _, event := range events {
		pbMsg, err := eventToProto(event.EventType, event.Payload)
		if err != nil {
			return err
		}
		if err := o.publisher.PublishEvent(ctx, event.PaymentID, event.EventType, pbMsg); err != nil {
			return err
		}

		ids = append(ids, event.ID)
	}

	if err := o.store.MarkEventsPublished(ctx, ids); err != nil {
		return err
	}
	return nil
}
