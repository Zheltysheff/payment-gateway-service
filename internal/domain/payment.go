package domain

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Status string

const (
	StatusNew        Status = "NEW"
	StatusProcessing Status = "PROCESSING"
	StatusCompleted  Status = "COMPLETED"
	StatusFailed     Status = "FAILED"
	StatusCanceled   Status = "CANCELED"
)

type Payment struct {
	ID         uuid.UUID
	Amount     int64
	Currency   string
	MerchantID string
	OrderID    string
	UserID     string
	Status     Status
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type Event interface {
	isPaymentEvent()
}

type PaymentCreatedEvent struct {
	PaymentID  uuid.UUID `json:"payment_id"`
	CommandID  uuid.UUID `json:"command_id"`
	Amount     int64     `json:"amount"`
	Currency   string    `json:"currency"`
	MerchantID string    `json:"merchant_id"`
	OrderID    string    `json:"order_id"`
	UserID     string    `json:"user_id"`
	OccurredAt time.Time `json:"occurred_at"`
}

func (e *PaymentCreatedEvent) isPaymentEvent() {}

const (
	EventTypePaymentCreated = "PaymentCreated"
)

func EventType(e Event) string {
	switch e.(type) {
	case *PaymentCreatedEvent:
		return EventTypePaymentCreated
	default:
		panic(fmt.Sprintf("unknown event type: %T", e))
	}
}

func NewEvent(eventType string) (Event, error) {
	switch eventType {
	case EventTypePaymentCreated:
		return &PaymentCreatedEvent{}, nil
	default:
		return nil, fmt.Errorf("unknown event type: %s", eventType)
	}
}

type CreatePaymentCommand struct {
	CommandID  uuid.UUID
	PaymentID  uuid.UUID
	Amount     int64
	Currency   string
	MerchantID string
	OrderID    string
	UserID     string
	IssuedAt   time.Time
}

func Apply(state *Payment, event Event) *Payment {
	switch e := event.(type) {
	case *PaymentCreatedEvent:
		return &Payment{ID: e.PaymentID, Amount: e.Amount, Currency: e.Currency, MerchantID: e.MerchantID,
			OrderID: e.OrderID, UserID: e.UserID, Status: StatusNew, CreatedAt: e.OccurredAt, UpdatedAt: e.OccurredAt}
	default:
		panic(fmt.Sprintf("unknown event type: %T", e))
	}
}

func Decide(state *Payment, cmd CreatePaymentCommand, now time.Time) ([]Event, error) {
	if state != nil {
		return nil, ErrIllegalStateChange
	}

	return []Event{&PaymentCreatedEvent{PaymentID: cmd.PaymentID, CommandID: cmd.CommandID, Amount: cmd.Amount,
		Currency: cmd.Currency, MerchantID: cmd.MerchantID, OrderID: cmd.OrderID, UserID: cmd.UserID, OccurredAt: now}}, nil
}

var ErrPaymentNotFound = errors.New("payment not found")
var ErrIllegalStateChange = errors.New("illegal state change")
