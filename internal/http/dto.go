package http

import (
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"

	"payment-gateway-service/internal/domain"
)

type createPaymentRequest struct {
	PaymentID  string `json:"payment_id"`
	Amount     int64  `json:"amount"`
	Currency   string `json:"currency"`
	MerchantID string `json:"merchant_id"`
	OrderID    string `json:"order_id"`
	UserID     string `json:"user_id"`
}

type createPaymentResponse struct {
	PaymentID string `json:"payment_id"`
	Status    string `json:"status"`
}

type paymentResponse struct {
	ID         string `json:"id"`
	Amount     int64  `json:"amount"`
	Currency   string `json:"currency"`
	MerchantID string `json:"merchant_id"`
	OrderID    string `json:"order_id"`
	UserID     string `json:"user_id"`
	Status     string `json:"status"`
	CreatedAt  string `json:"created_at"`
	UpdatedAt  string `json:"updated_at"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func (r createPaymentRequest) validate() (uuid.UUID, error) {
	if strings.TrimSpace(r.PaymentID) == "" {
		return uuid.Nil, errors.New("payment_id is required")
	}
	id, err := uuid.Parse(r.PaymentID)
	if err != nil {
		return uuid.Nil, errors.New("payment_id must be a valid UUID")
	}
	if r.Amount <= 0 {
		return uuid.Nil, errors.New("amount must be positive")
	}
	if len(r.Currency) != 3 {
		return uuid.Nil, errors.New("currency must be a 3-letter ISO-4217 code")
	}
	if strings.TrimSpace(r.MerchantID) == "" {
		return uuid.Nil, errors.New("merchant_id is required")
	}
	if strings.TrimSpace(r.OrderID) == "" {
		return uuid.Nil, errors.New("order_id is required")
	}
	if strings.TrimSpace(r.UserID) == "" {
		return uuid.Nil, errors.New("user_id is required")
	}
	return id, nil
}

func toResponse(p *domain.Payment) paymentResponse {
	return paymentResponse{
		ID:         p.ID.String(),
		Amount:     p.Amount,
		Currency:   p.Currency,
		MerchantID: p.MerchantID,
		OrderID:    p.OrderID,
		UserID:     p.UserID,
		Status:     string(p.Status),
		CreatedAt:  p.CreatedAt.Format(time.RFC3339),
		UpdatedAt:  p.UpdatedAt.Format(time.RFC3339),
	}
}
