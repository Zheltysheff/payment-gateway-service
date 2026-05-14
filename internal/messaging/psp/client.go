package psp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type Client struct {
	baseURL     string
	callbackURL string
	httpClient  *http.Client
}

type paymentRequest struct {
	PaymentID   string `json:"payment_id"`
	Amount      int64  `json:"amount"`
	Currency    string `json:"currency"`
	CallbackURL string `json:"callback_url"`
}

func New(baseURL, callbackURL string) *Client {
	return &Client{
		baseURL:     baseURL,
		callbackURL: callbackURL,
		httpClient:  &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *Client) Payment(ctx context.Context, paymentID uuid.UUID, amount int64, currency string) error {
	tracer := otel.Tracer("psp.client")
	ctx, span := tracer.Start(ctx, "psp.payment", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	req := paymentRequest{
		PaymentID:   paymentID.String(),
		Amount:      amount,
		Currency:    currency,
		CallbackURL: c.callbackURL,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/payment", bytes.NewReader(body))
	if err != nil {
		return err
	}

	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(httpReq.Header))

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("psp returned status %d", resp.StatusCode)
	}

	return nil
}
