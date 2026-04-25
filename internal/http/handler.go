package http

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/google/uuid"

	"payment-gateway-service/internal/domain"
	"payment-gateway-service/internal/service/api"
)

type PaymentService interface {
	CreatePayment(ctx context.Context, cmd api.CreatePaymentCommand) error
	GetPayment(ctx context.Context, id uuid.UUID) (*domain.Payment, error)
}

type Handler struct {
	svc    PaymentService
	logger *slog.Logger
}

func NewHandler(svc PaymentService, logger *slog.Logger) *Handler {
	return &Handler{svc: svc, logger: logger}
}

func (h *Handler) CreatePayment(w http.ResponseWriter, r *http.Request) {
	var req createPaymentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid json body")
		return
	}

	id, err := req.validate()
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	cmd := api.CreatePaymentCommand{
		PaymentID:  id,
		Amount:     req.Amount,
		Currency:   strings.ToUpper(req.Currency),
		MerchantID: req.MerchantID,
		OrderID:    req.OrderID,
		UserID:     req.UserID,
	}

	if err := h.svc.CreatePayment(r.Context(), cmd); err != nil {
		h.logger.ErrorContext(r.Context(), "Publish create payment command", "error", err, "payment_id", id)
		writeError(w, http.StatusInternalServerError, "Failed to accept payment")
		return
	}

	writeJSON(w, http.StatusAccepted, createPaymentResponse{
		PaymentID: id.String(),
		Status:    string(domain.StatusNew),
	})
}

func (h *Handler) GetPayment(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ID must be a valid UUID")
		return
	}

	payment, err := h.svc.GetPayment(r.Context(), id)
	if errors.Is(err, domain.ErrPaymentNotFound) {
		writeError(w, http.StatusNotFound, "Payment not found")
		return
	}
	if err != nil {
		h.logger.ErrorContext(r.Context(), "Get payment", "error", err, "payment_id", id)
		writeError(w, http.StatusInternalServerError, "Internal error")
		return
	}

	writeJSON(w, http.StatusOK, toResponse(payment))
}

func (h *Handler) Health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}
