package worker

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"payment-gateway-service/internal/domain"
)

type CommandType string

const (
	CmdCreatePayment CommandType = "CreatePayment"
)

type EventStore interface {
	Tx(ctx context.Context, fn func(tx EventStoreTx) error) error
}

type EventStoreTx interface {
	IsCommandProcessed(ctx context.Context, commandID uuid.UUID) (bool, error)
	LoadEvents(ctx context.Context, paymentID uuid.UUID) ([]domain.Event, error)
	AppendEvents(ctx context.Context, paymentID uuid.UUID, events []domain.Event) error
	MarkCommandProcessed(ctx context.Context, commandID uuid.UUID, paymentID uuid.UUID, commandType CommandType) error
}

type PaymentService struct {
	store EventStore
}

func New(store EventStore) *PaymentService {
	return &PaymentService{store: store}
}

func (s *PaymentService) HandleCreatePayment(ctx context.Context, cmd domain.CreatePaymentCommand) error {
	return s.store.Tx(ctx, func(tx EventStoreTx) error {
		processed, err := tx.IsCommandProcessed(ctx, cmd.CommandID)
		if err != nil {
			return err
		}
		if processed {
			return nil
		}

		events, err := tx.LoadEvents(ctx, cmd.PaymentID)
		if err != nil {
			return err
		}

		var state *domain.Payment
		for _, e := range events {
			state = domain.Apply(state, e)
		}

		newEvents, err := domain.Decide(state, cmd, time.Now().UTC())
		if errors.Is(err, domain.ErrIllegalStateChange) {
			return tx.MarkCommandProcessed(ctx, cmd.CommandID, cmd.PaymentID, CmdCreatePayment)
		}

		if err != nil {
			return err
		}

		if err := tx.AppendEvents(ctx, cmd.PaymentID, newEvents); err != nil {
			return err
		}

		return tx.MarkCommandProcessed(ctx, cmd.CommandID, cmd.PaymentID, CmdCreatePayment)
	})
}
