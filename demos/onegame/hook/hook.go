package hook

import (
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"
)

type TraceIdHook struct {
	TraceId string
	SpanId  string
	Span    trace.Span
}

func NewTraceIdHook(traceId, spanId string, span trace.Span) logrus.Hook {
	hook := TraceIdHook{
		TraceId: traceId,
		SpanId:  spanId,
		Span:    span,
	}
	return &hook
}

func (hook *TraceIdHook) Fire(entry *logrus.Entry) error {
	entry.Data["traceId"] = hook.TraceId
	entry.Data["spanId"] = hook.SpanId
	// entry.Data["span"] = fmt.Sprintf("%#v", hook.Span)
	return nil
}

func (hook *TraceIdHook) Levels() []logrus.Level {
	return logrus.AllLevels
}
