FROM golang:1.25.6 AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /api -trimpath -ldflags="-s -w" ./cmd/api

FROM gcr.io/distroless/static-debian12:nonroot AS build-release-stage

WORKDIR /

COPY --from=builder /api /api

EXPOSE 8080

ENTRYPOINT [ "/api" ]