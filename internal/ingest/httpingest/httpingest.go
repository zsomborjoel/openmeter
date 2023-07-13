package httpingest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/go-chi/render"
	"golang.org/x/exp/slog"

	"github.com/openmeterio/openmeter/api"
)

// Handler receives an event in CloudEvents format and forwards it to a {Collector}.
type Handler struct {
	Collector Collector

	Logger *slog.Logger
}

// Collector is a receiver of events that handles sending those events to some downstream broker.
type Collector interface {
	Receive(ev event.Event) error
}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger := h.getLogger()

	contentType := r.Header.Get("Content-Type")

	var err error
	switch contentType {
	case "application/cloudevents+json":
		err = h.processSingleRequest(w, r)
	case "application/cloudevents-batch+json":
		err = h.processBatchRequest(w, r)
	default:
		_ = render.Render(w, r, api.ErrUnsupportedMediaType(errors.New("content type must be application/cloudevents+json or application/cloudevents-batch+json")))
	}

	if err != nil {
		logger.ErrorCtx(r.Context(), "unable to process request", "error", err)
		_ = render.Render(w, r, api.ErrInternalServerError(err))
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h Handler) processBatchRequest(w http.ResponseWriter, r *http.Request) error {
	var events []event.Event

	err := json.NewDecoder(r.Body).Decode(&events)
	if err != nil {
		return err
	}

	errChan := make(chan error, len(events))
	var wg sync.WaitGroup
	wg.Add(len(events))

	for _, event := range events {
		go func(event api.Event) {
			defer wg.Done()
			errChan <- h.processEvent(r.Context(), event)
		}(event)
	}

	wg.Wait()
	close(errChan)

	var errs []error
	for err := range errChan {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

func (h Handler) processSingleRequest(w http.ResponseWriter, r *http.Request) error {
	var event event.Event

	err := json.NewDecoder(r.Body).Decode(&event)
	if err != nil {
		return err
	}

	err = h.processEvent(r.Context(), event)
	if err != nil {
		return err
	}

	return nil
}

func (h Handler) processEvent(context context.Context, event event.Event) error {
	logger := h.getLogger()

	logger = logger.With(
		slog.String("event_id", event.ID()),
		slog.String("event_subject", event.Subject()),
		slog.String("event_source", event.Source()),
	)

	if event.Time().IsZero() {
		logger.DebugCtx(context, "event does not have a timestamp")
		event.SetTime(time.Now().UTC())
	}

	err := h.Collector.Receive(event)
	if err != nil {
		return err
	}

	logger.InfoCtx(context, "event forwarded to downstream collector")
	return nil
}

func (h Handler) getLogger() *slog.Logger {
	logger := h.Logger

	if logger == nil {
		logger = slog.Default()
	}

	return logger
}
