package telemetry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

const (
	defaultCompressor = "gzip"

	defaultTimeout = 5 * time.Second

	defaultRetryInitInterval = 500 * time.Millisecond
	defaultRetryMaxInterval  = 5 * time.Second
	defaultRetryMaxElapsed   = 30 * time.Second

	defaultSamplingRatio = 1.0
)

type Config struct {
	CollectorEndpoint string
	ServiceName       string
	ServiceVersion    string
	Environment       string
	SamplingRatio     float64 // 0 means 1.0, that means to write all spans
}

func InitTracer(ctx context.Context, cfg Config) (func(context.Context) error, error) {
	samplingRatio := cfg.SamplingRatio
	if samplingRatio == 0 {
		samplingRatio = defaultSamplingRatio
	}

	// OTLP gRPC exprorter - sends the spans to the collector (Jaeger, Tempo, OTEL Collector и т.д.)
	exporter, err := otlptracegrpc.New(
		ctx,
		otlptracegrpc.WithEndpoint(cfg.CollectorEndpoint),
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithTimeout(defaultTimeout),
		otlptracegrpc.WithCompressor(defaultCompressor),
		otlptracegrpc.WithRetry(otlptracegrpc.RetryConfig{
			Enabled:         true,
			InitialInterval: defaultRetryInitInterval,
			MaxInterval:     defaultRetryMaxInterval,
			MaxElapsedTime:  defaultRetryMaxElapsed,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create OTLP exporter: %w", err)
	}

	// OTLP resource add to the spans configured options
	res, err := resource.New(
		ctx,

		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion(cfg.ServiceVersion),
			semconv.DeploymentEnvironmentName(cfg.Environment),
		),

		resource.WithHost(),
		resource.WithOS(),
		resource.WithProcess(),
		resource.WithContainer(),
		resource.WithTelemetrySDK(),
	)

	if err != nil && !errors.Is(err, resource.ErrPartialResource) {
		return nil, fmt.Errorf("create resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(samplingRatio))),
	)

	otel.SetTracerProvider(tp)

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
	))

	return tp.Shutdown, nil
}
