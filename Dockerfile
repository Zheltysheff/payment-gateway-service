ARG GO_VERSION=1.25.4
ARG OS_VERSION=3.20

FROM golang:${GO_VERSION}-alpine AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG BUILD_TARGET=api
RUN CGO_ENABLED=0 \
    go build -trimpath -ldflags="-s -w" \
      -o /out/service ./cmd/${BUILD_TARGET}

FROM alpine:${OS_VERSION}
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=builder /out/service /app/service

ENTRYPOINT ["/app/service"]
