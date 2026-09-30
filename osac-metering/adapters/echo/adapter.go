/*
Copyright (c) 2026 Red Hat, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0
*/

package echo

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/osac-project/osac-metering/adapters"
)

// Adapter is the echo provider. It implements adapters.ProviderAdapter and
// exposes the event query handlers used by the echo-adapter binary.
type Adapter struct {
	store     *eventStore
	submitted atomic.Int64
	flushed   atomic.Int64
}

// NewAdapter creates an echo provider adapter with a bounded event store.
// bufferSize <= 0 uses DefaultMaxEvents.
func NewAdapter(bufferSize int) *Adapter {
	return &Adapter{store: newEventStore(bufferSize)}
}

func (a *Adapter) Name() string { return "echo" }

func (a *Adapter) Submit(_ context.Context, event adapters.MeteringEvent) error {
	fmt.Printf("[SUBMIT] id=%-36s type=%-30s topic=%-30s partition=%d offset=%d\n",
		event.CloudEvent.ID(),
		event.CloudEvent.Type(),
		event.Topic,
		event.Partition,
		event.Offset,
	)
	a.store.add(event)
	a.submitted.Add(1)
	return nil
}

func (a *Adapter) Flush(_ context.Context) (adapters.SubmitResult, error) {
	n := a.flushed.Add(1)
	total := a.submitted.Load()
	fmt.Printf("[FLUSH]  #%d — %d events submitted so far\n", n, total)
	return adapters.SubmitResult{Idempotent: true}, nil
}

func (a *Adapter) HealthCheck(_ context.Context) error { return nil }

func (a *Adapter) Close() error {
	fmt.Printf("[CLOSE]  total events submitted: %d, total flushes: %d\n",
		a.submitted.Load(), a.flushed.Load())
	return nil
}

func (a *Adapter) HandleEvents(w http.ResponseWriter, r *http.Request) {
	a.store.handleEvents(w, r)
}

func (a *Adapter) HandleDeleteEvents(w http.ResponseWriter, r *http.Request) {
	a.store.handleDeleteEvents(w, r)
}

func (a *Adapter) HandleCount(w http.ResponseWriter, r *http.Request) {
	a.store.handleCount(w, r)
}

func (a *Adapter) HandleEventByID(w http.ResponseWriter, r *http.Request) {
	a.store.handleEventByID(w, r)
}
