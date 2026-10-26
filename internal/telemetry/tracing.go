package telemetry

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/votiakov/airq/internal/version"
)

// defaultAttributeValueLimit caps span attribute values (SQL text, error messages)
// unless OTEL_ATTRIBUTE_VALUE_LENGTH_LIMIT says otherwise. The SDK default is
// unlimited, and a 100-row batch INSERT is several kilobytes of SQL.
const defaultAttributeValueLimit = 2048

// SetupTracing installs the global OpenTelemetry tracer provider and the W3C Trace
// Context propagator.
//
// Spans are always created, so trace and span IDs reach logs, metric exemplars and
// X-Request-ID. They are exported over OTLP/HTTP only when OTEL_EXPORTER_OTLP_ENDPOINT
// or OTEL_EXPORTER_OTLP_TRACES_ENDPOINT is set; the exporter, sampler
// (OTEL_TRACES_SAMPLER) and resource (OTEL_SERVICE_NAME, OTEL_RESOURCE_ATTRIBUTES)
// follow the standard OTEL_* environment variables.
//
// Export and SDK errors are logged to logger. The returned provider must be shut down
// to flush buffered spans.
func SetupTracing(ctx context.Context, getenv func(string) string, logger *slog.Logger) (*sdktrace.TracerProvider, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName("airq"), semconv.ServiceVersion(version.Get().Version)),
		resource.WithFromEnv(), // after the defaults so OTEL_SERVICE_NAME can override them
		resource.WithTelemetrySDK(),
		resource.WithHost(),
	)
	if err != nil {
		return nil, fmt.Errorf("tracing resource: %w", err)
	}

	limits := sdktrace.NewSpanLimits() // honours OTEL_SPAN_* / OTEL_ATTRIBUTE_* variables
	if limits.AttributeValueLengthLimit < 0 {
		limits.AttributeValueLengthLimit = defaultAttributeValueLimit
	}
	opts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res), sdktrace.WithRawSpanLimits(limits)}
	if getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != "" {
		exp, err := otlptracehttp.New(ctx)
		if err != nil {
			return nil, fmt.Errorf("otlp exporter: %w", err)
		}
		opts = append(opts, sdktrace.WithBatcher(exp))
	}

	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		logger.WarnContext(ctx, "opentelemetry error", "error", err)
	}))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp, nil
}
