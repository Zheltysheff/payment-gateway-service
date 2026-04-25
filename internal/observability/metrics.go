package observability

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type HTTPMetrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func NewRegistry() *prometheus.Registry {
	r := prometheus.NewRegistry()
	r.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return r
}

func NewHTTPMetrics(r *prometheus.Registry) *HTTPMetrics {
	m := &HTTPMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total HTTP requests by route, method, and status.",
		}, []string{"route", "method", "status"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency by route, method, and status.",
			Buckets: prometheus.DefBuckets,
		}, []string{"route", "method", "status"}),
	}
	r.MustRegister(m.requests, m.duration)
	return m
}

func (m *HTTPMetrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)

		route := r.Pattern
		if route == "" {
			route = r.URL.Path
		}
		status := strconv.Itoa(rw.status)
		m.requests.WithLabelValues(route, r.Method, status).Inc()
		m.duration.WithLabelValues(route, r.Method, status).Observe(time.Since(start).Seconds())
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (s *statusRecorder) WriteHeader(code int) {
	if s.wroteHeader {
		return
	}
	s.status = code
	s.wroteHeader = true
	s.ResponseWriter.WriteHeader(code)
}
