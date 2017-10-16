package main

import (
	"fmt"
	"time"
)

var (
	totalSubscriptions = 20000000
	totalEvents = totalSubscriptions * 3
)

type Subscription struct{
	ID int
	Subtype string
	IsCreated bool
	IsFulfilled bool
	IsClosed bool
	Version int
	Created time.Time
	Updated time.Time
}

type Event struct{
	ID int
	SourceID int
	Subtype string
	Data EventData
}

type EventData struct{
	Subtype string
	Version int
}

func filter(subscriptions []Subscription, fn func(subscription Subscription) bool) []Subscription {
	res := make([]Subscription, 0, totalSubscriptions)

	for _, subscription := range subscriptions {
		if fn(subscription) {
			res = append(res, subscription)
		}
	}

	return res
}

func makeEvents() []Event {
	events := make([]Event, 0, totalEvents)

	start := time.Now()
	for i := 0; i < totalSubscriptions; i++ {
		events = append(events, Event{
			ID: i,
			SourceID: i,
			Subtype: "subscription.created",
			Data: EventData{
				Subtype: "incomplete-profile",
				Version: 0,
			},
		})
		events = append(events, Event{
			ID: i,
			SourceID: i,
			Subtype: "subscription.fulfilled",
			Data: EventData{
				Subtype: "incomplete-profile",
				Version: 1,
			},
		})
		events = append(events, Event{
			ID: i,
			SourceID: i,
			Subtype: "subscription.closed",
			Data: EventData{
				Subtype: "incomplete-profile",
				Version: 2,
			},
		})
	}
	duration := time.Since(start)

	elapsed := duration / time.Millisecond
	rps := totalEvents / int(elapsed)

	fmt.Printf("added %d events in %d milliseconds @ %d/ms\r\n", totalEvents, elapsed, rps)

	return events
}

func applyEvents(events []Event) []Subscription {
	index := make([]Subscription, totalEvents, totalEvents)

	start := time.Now()
	for _, event := range events {
		now := time.Now()

		subscription := &index[event.SourceID]

		switch event.Subtype {
		case "subscription.created":
			subscription.ID = event.SourceID
			subscription.Subtype = event.Data.Subtype
			subscription.IsCreated = true
			subscription.Version = event.Data.Version
			subscription.Created = now
			subscription.Updated = now
		case "subscription.fulfilled":
			subscription.IsFulfilled = true
			subscription.Version = event.Data.Version
			subscription.Updated = now
		case "subscription.closed":
			subscription.IsClosed = true
			subscription.Version = event.Data.Version
			subscription.Updated = now
		}
	}
	duration := time.Since(start)

	elapsed := duration / time.Millisecond
	rps := totalEvents / int(elapsed)

	fmt.Printf("applied %d events in %d milliseconds @ %d/ms\r\n", totalEvents, elapsed, rps)

	return index
}

func filterIndex(index []Subscription) [] Subscription {
	start := time.Now()
	filtered := filter(index, func(subscription Subscription) bool {
		return subscription.IsCreated
	})
	duration := time.Since(start)

	elapsed := duration / time.Millisecond
	rps := totalEvents / int(elapsed)

	fmt.Printf("filtered %d subscriptions in %d milliseconds @ %d/ms\r\n", len(filtered), elapsed, rps)

	return filtered
}

func main() {
	// todo - run commands to generate events

	// make raw events slice to consume
	events := makeEvents()

	// todo - journal events

	// apply events to aggregates in index
	index := applyEvents(events)

	// filter aggregates index by field values
	_ = filterIndex(index)

	// print single aggregate
	fmt.Printf("%+v\r\n", index[totalSubscriptions - 1])
}
