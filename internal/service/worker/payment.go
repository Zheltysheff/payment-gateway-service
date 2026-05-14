package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"payment-gateway-service/internal/domain"
)

type EventStore interface {
	Tx(ctx context.Context, fn func(tx EventStoreTx) error) error
}

type EventStoreTx interface {
	IsCommandProcessed(ctx context.Context, commandID uuid.UUID) (bool, error)
	LoadEvents(ctx context.Context, paymentID uuid.UUID) ([]domain.Event, error)
	AppendEvents(ctx context.Context, paymentID uuid.UUID, events []domain.Event) error
	MarkCommandProcessed(ctx context.Context, commandID uuid.UUID, paymentID uuid.UUID, commandType domain.CommandType) error
}

type PSPClient interface {
	Payment(ctx context.Context, paymentID uuid.UUID, amount int64, currency string) error
}

type PaymentService struct {
	store  EventStore
	psp    PSPClient
	logger *slog.Logger
}

func New(store EventStore, psp PSPClient, logger *slog.Logger) *PaymentService {
	return &PaymentService{store: store, psp: psp, logger: logger}
}

func (s *PaymentService) HandleCreatePayment(ctx context.Context, cmd domain.CreatePaymentCommand) error {
	var shouldCallPSP bool
	err := s.store.Tx(ctx, func(tx EventStoreTx) error {
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

		newEvents, err := domain.Decide(state, &cmd, time.Now().UTC())
		if errors.Is(err, domain.ErrIllegalStateChange) {
			return tx.MarkCommandProcessed(ctx, cmd.CommandID, cmd.PaymentID, domain.CmdCreatePayment)
		}

		if err != nil {
			return err
		}

		if err := tx.AppendEvents(ctx, cmd.PaymentID, newEvents); err != nil {
			return err
		}

		if err := tx.MarkCommandProcessed(ctx, cmd.CommandID, cmd.PaymentID, domain.CmdCreatePayment); err != nil {
			return err
		}
		shouldCallPSP = true
		return nil
	})

	if err != nil {
		return err
	}

	if shouldCallPSP {
		if err := s.psp.Payment(ctx, cmd.PaymentID, cmd.Amount, cmd.Currency); err != nil {
			s.logger.WarnContext(ctx, "psp call failed", "error", err, "payment_id", cmd.PaymentID)
		}
	}
	return nil
}

func (s *PaymentService) HandleMarkCompleted(ctx context.Context, cmd domain.MarkCompletedCommand) error {
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

		newEvents, err := domain.Decide(state, &cmd, time.Now().UTC())
		if errors.Is(err, domain.ErrIllegalStateChange) {
			return tx.MarkCommandProcessed(ctx, cmd.CommandID, cmd.PaymentID, domain.CmdMarkCompleted)
		}

		if err != nil {
			return err
		}

		if err := tx.AppendEvents(ctx, cmd.PaymentID, newEvents); err != nil {
			return err
		}

		return tx.MarkCommandProcessed(ctx, cmd.CommandID, cmd.PaymentID, domain.CmdMarkCompleted)
	})
}

func (s *PaymentService) HandleMarkFailed(ctx context.Context, cmd domain.MarkFailedCommand) error {
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

		newEvents, err := domain.Decide(state, &cmd, time.Now().UTC())
		if errors.Is(err, domain.ErrIllegalStateChange) {
			return tx.MarkCommandProcessed(ctx, cmd.CommandID, cmd.PaymentID, domain.CmdMarkFailed)
		}

		if err != nil {
			return err
		}

		if err := tx.AppendEvents(ctx, cmd.PaymentID, newEvents); err != nil {
			return err
		}

		return tx.MarkCommandProcessed(ctx, cmd.CommandID, cmd.PaymentID, domain.CmdMarkFailed)
	})
}
