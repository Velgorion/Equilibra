FROM golang:1.26.0 AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /api -trimpath -ldflags="-s -w" ./cmd/api

RUN CGO_ENABLED=0 GOOS=linux go build -o /relay -trimpath -ldflags="-s -w" ./cmd/relay

FROM gcr.io/distroless/static-debian12:nonroot AS api

WORKDIR /

COPY --from=builder /api /api

EXPOSE 8080

ENTRYPOINT [ "/api" ]

FROM gcr.io/distroless/static-debian12:nonroot AS relay

WORKDIR /

COPY --from=builder /relay /relay

ENTRYPOINT [ "/relay" ]