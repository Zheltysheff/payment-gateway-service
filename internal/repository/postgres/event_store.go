package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"payment-gateway-service/internal/domain"
	"payment-gateway-service/internal/service/worker"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type EventStoreRepository struct {
	pool *pgxpool.Pool
}

func NewEventStoreRepository(pool *pgxpool.Pool) *EventStoreRepository {
	return &EventStoreRepository{pool: pool}
}

type eventStoreTx struct {
	tx pgx.Tx
}

func (s *EventStoreRepository) Tx(ctx context.Context, fn func(tx worker.EventStoreTx) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if err := fn(&eventStoreTx{tx: tx}); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

const existsCommandQuery = `
SELECT EXISTS(SELECT 1 FROM processed_commands WHERE command_id = $1)
`

const getEventByIdQuery = `
SELECT event_type, payload FROM payment_events WHERE payment_id = $1 ORDER BY id
`

const insertEventsQuery = `
INSERT INTO payment_events (payment_id, event_type, payload) VALUES ($1, $2, $3)
`

const insertCommandQuery = `
INSERT INTO processed_commands (command_id, payment_id, command_type) VALUES ($1, $2, $3)
`

func (t *eventStoreTx) IsCommandProcessed(ctx context.Context, commandID uuid.UUID) (bool, error) {
	var exists bool
	if err := t.tx.QueryRow(ctx, existsCommandQuery, commandID).Scan(&exists); err != nil {
		return false, fmt.Errorf("check command %s processed: %w", commandID, err)
	}
	return exists, nil
}

func (t *eventStoreTx) LoadEvents(ctx context.Context, paymentID uuid.UUID) ([]domain.Event, error) {
	rows, err := t.tx.Query(ctx, getEventByIdQuery, paymentID)
	if err != nil {
		return nil, fmt.Errorf("load events for payment %s: %w", paymentID, err)
	}
	defer rows.Close()

	var events []domain.Event

	for rows.Next() {
		var eventType string
		var payload []byte
		if err := rows.Scan(&eventType, &payload); err != nil {
			return nil, fmt.Errorf("load events for payment %s: scan row: %w", paymentID, err)
		}

		e, err := domain.NewEvent(eventType)
		if err != nil {
			return nil, fmt.Errorf("load events for payment %s: unknown event type %q: %w", paymentID, eventType, err)
		}

		if err := json.Unmarshal(payload, e); err != nil {
			return nil, fmt.Errorf("load events for payment %s: unmarshal payload: %w", paymentID, err)
		}

		events = append(events, e)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load events for payment %s: rows iter: %w", paymentID, err)
	}

	return events, nil
}

func (t *eventStoreTx) AppendEvents(ctx context.Context, paymentID uuid.UUID, events []domain.Event) error {
	for _, e := range events {
		payload, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("append events for payment %s: marshal payload: %w", paymentID, err)
		}

		eventType := domain.EventType(e)

		if _, err := t.tx.Exec(ctx, insertEventsQuery, paymentID, eventType, payload); err != nil {
			return fmt.Errorf("append events for payment %s: insert event %q: %w", paymentID, eventType, err)
		}
	}
	return nil
}

func (t *eventStoreTx) MarkCommandProcessed(
	ctx context.Context,
	commandID uuid.UUID,
	paymentID uuid.UUID,
	commandType worker.CommandType) error {
	if _, err := t.tx.Exec(ctx, insertCommandQuery, commandID, paymentID, string(commandType)); err != nil {
		return fmt.Errorf("mark command %s (%s) processed: %w", commandID, commandType, err)
	}
	return nil
}
