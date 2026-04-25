package http

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	metrics "payment-gateway-service/internal/observability"
)

func NewRouter(h *Handler, registry *prometheus.Registry, metrics *metrics.HTTPMetrics) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/payments", h.CreatePayment)
	mux.HandleFunc("GET /api/payments/{id}", h.GetPayment)
	mux.HandleFunc("GET /health", h.Health)
	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{Registry: registry}))

	wrapped := metrics.Middleware(mux)
	wrapped = otelhttp.NewHandler(wrapped, "http.server",
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
			return r.Method + " " + r.URL.Path
		}),
	)
	return wrapped
}
