package domain

import (
	"errors"
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

var ErrPaymentNotFound = errors.New("Payment not found")
