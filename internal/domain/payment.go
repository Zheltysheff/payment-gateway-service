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

type PaymentCompletedEvent struct {
	PaymentID  uuid.UUID `json:"payment_id"`
	CommandID  uuid.UUID `json:"command_id"`
	OccurredAt time.Time `json:"occurred_at"`
}

type PaymentFailedEvent struct {
	PaymentID  uuid.UUID `json:"payment_id"`
	CommandID  uuid.UUID `json:"command_id"`
	Reason     string    `json:"reason"`
	OccurredAt time.Time `json:"occurred_at"`
}

type Command interface {
	isPaymentCommand()
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

type MarkCompletedCommand struct {
	CommandID uuid.UUID
	PaymentID uuid.UUID
	IssuedAt  time.Time
}

type MarkFailedCommand struct {
	CommandID uuid.UUID
	PaymentID uuid.UUID
	Reason    string
	IssuedAt  time.Time
}

func (e *PaymentCreatedEvent) isPaymentEvent()   {}
func (e *PaymentCompletedEvent) isPaymentEvent() {}
func (e *PaymentFailedEvent) isPaymentEvent()    {}

func (c *CreatePaymentCommand) isPaymentCommand() {}
func (c *MarkCompletedCommand) isPaymentCommand() {}
func (c *MarkFailedCommand) isPaymentCommand()    {}

const (
	EventTypePaymentCreated   = "PaymentCreated"
	EventTypePaymentCompleted = "PaymentCompleted"
	EventTypePaymentFailed    = "PaymentFailed"
)

type CommandType string

const (
	CmdCreatePayment CommandType = "CreatePayment"
	CmdMarkCompleted CommandType = "MarkCompleted"
	CmdMarkFailed    CommandType = "MarkFailed"
)

func EventType(e Event) string {
	switch e.(type) {
	case *PaymentCreatedEvent:
		return EventTypePaymentCreated
	case *PaymentCompletedEvent:
		return EventTypePaymentCompleted
	case *PaymentFailedEvent:
		return EventTypePaymentFailed
	default:
		panic(fmt.Sprintf("unknown event type: %T", e))
	}
}

func NewEvent(eventType string) (Event, error) {
	switch eventType {
	case EventTypePaymentCreated:
		return &PaymentCreatedEvent{}, nil
	case EventTypePaymentCompleted:
		return &PaymentCompletedEvent{}, nil
	case EventTypePaymentFailed:
		return &PaymentFailedEvent{}, nil
	default:
		return nil, fmt.Errorf("unknown event type: %s", eventType)
	}
}

func Apply(state *Payment, event Event) *Payment {
	switch e := event.(type) {
	case *PaymentCreatedEvent:
		return &Payment{ID: e.PaymentID, Amount: e.Amount, Currency: e.Currency, MerchantID: e.MerchantID,
			OrderID: e.OrderID, UserID: e.UserID, Status: StatusNew, CreatedAt: e.OccurredAt, UpdatedAt: e.OccurredAt}
	case *PaymentCompletedEvent:
		next := *state
		next.Status = StatusCompleted
		next.UpdatedAt = e.OccurredAt
		return &next
	case *PaymentFailedEvent:
		next := *state
		next.Status = StatusFailed
		next.UpdatedAt = e.OccurredAt
		return &next
	default:
		panic(fmt.Sprintf("unknown event type: %T", e))
	}
}

func Decide(state *Payment, cmd Command, now time.Time) ([]Event, error) {
	switch c := cmd.(type) {
	case *CreatePaymentCommand:
		if state != nil {
			return nil, ErrIllegalStateChange
		}

		return []Event{&PaymentCreatedEvent{PaymentID: c.PaymentID, CommandID: c.CommandID, Amount: c.Amount,
			Currency: c.Currency, MerchantID: c.MerchantID, OrderID: c.OrderID, UserID: c.UserID, OccurredAt: now}}, nil
	case *MarkCompletedCommand:
		if state == nil {
			return nil, ErrPaymentNotFound
		}
		if state.Status != StatusNew {
			return nil, ErrIllegalStateChange
		}

		return []Event{&PaymentCompletedEvent{PaymentID: c.PaymentID, CommandID: c.CommandID, OccurredAt: now}}, nil
	case *MarkFailedCommand:
		if state == nil {
			return nil, ErrPaymentNotFound
		}
		if state.Status != StatusNew {
			return nil, ErrIllegalStateChange
		}

		return []Event{&PaymentFailedEvent{PaymentID: c.PaymentID, CommandID: c.CommandID, Reason: c.Reason, OccurredAt: now}}, nil
	default:
		return nil, fmt.Errorf("unknown command type: %T", c)
	}
}

var ErrPaymentNotFound = errors.New("payment not found")
var ErrIllegalStateChange = errors.New("illegal state change")
