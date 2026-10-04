package host

import (
	"fmt"
	"sort"
	"sync"

	"extsys/internal/protocol"
)

func buildSubscriptionPreviews(subs []protocol.SubscriptionScope, events []protocol.EventProgress, accepted, rejected []string) []SubscriptionPreview {
	acceptedSet := map[string]bool{}
	for _, id := range accepted {
		acceptedSet[id] = true
	}
	rejectedSet := map[string]bool{}
	for _, id := range rejected {
		rejectedSet[id] = true
	}
	sort.Slice(subs, func(i, j int) bool { return subs[i].ID < subs[j].ID })
	bySub := map[string][]protocol.EventProgress{}
	for _, ev := range events {
		bySub[ev.SubscriptionID] = append(bySub[ev.SubscriptionID], ev)
	}
	out := make([]SubscriptionPreview, 0, len(subs))
	for _, sub := range subs {
		events := bySub[sub.ID]
		sort.Slice(events, func(i, j int) bool { return events[i].Position < events[j].Position })
		p := SubscriptionPreview{
			Scope:    sub,
			Accepted: acceptedSet[sub.ID],
			Events:   events,
		}
		for _, ev := range events {
			if ev.Status == "confirmed" {
				p.Confirmed = append(p.Confirmed, ev.EventID)
			} else {
				p.InFlight = append(p.InFlight, ev.EventID)
			}
		}
		if rejectedSet[sub.ID] {
			p.Uncertainty = "candidate cannot interpret this subscription scope"
		} else if !acceptedSet[sub.ID] {
			p.Uncertainty = "candidate did not report this subscription scope"
		} else if len(p.InFlight) > 0 {
			p.Uncertainty = fmt.Sprintf("%d event(s) have not been confirmed by the old version", len(p.InFlight))
		}
		p.SafeToContinue = p.Uncertainty == ""
		out = append(out, p)
	}
	return out
}

func subscriptionDecision(decisions map[string]Disposition, ext string, subID string) Disposition {
	if d := decisions["subscription:"+subID]; d != "" {
		return d
	}
	return decisions[ext]
}

func (m *Manager) transferSubscriptions(item *stagedItem, previews []SubscriptionPreview, decisions map[string]Disposition) error {
	old, next := item.old, item.new
	subs, events, err := old.querySubscriptions(m.nextReqID(), m.cfg.TaskTimeout)
	if err != nil {
		return fmt.Errorf("re-dump %s subscriptions before cutover: %w", item.name, err)
	}
	accepted, rejected, err := next.checkSubscriptions(m.nextReqID(), subs, m.cfg.TaskTimeout)
	if err != nil {
		return fmt.Errorf("re-check %s subscriptions before cutover: %w", item.name, err)
	}
	fresh := buildSubscriptionPreviews(subs, events, accepted, rejected)
	stopped := map[string]bool{}
	for _, p := range fresh {
		d := subscriptionDecision(decisions, item.name, p.Scope.ID)
		if !p.SafeToContinue {
			if d != DecideStopSubscription {
				return fmt.Errorf("subscription %s cannot safely continue: %s; cancel upgrade or stop the subscription explicitly", p.Scope.ID, p.Uncertainty)
			}
			stopped[p.Scope.ID] = true
		}
	}
	var live []protocol.SubscriptionScope
	confirmed := map[string]protocol.EventProgress{}
	for _, p := range fresh {
		if stopped[p.Scope.ID] || subscriptionDecision(decisions, item.name, p.Scope.ID) == DecideStopSubscription {
			continue
		}
		live = append(live, p.Scope)
		for _, ev := range p.Events {
			if ev.Status == "confirmed" {
				confirmed[eventKey(ev)] = ev
			}
		}
	}
	if err := next.loadSubscriptions(m.nextReqID(), live, m.cfg.TaskTimeout); err != nil {
		return fmt.Errorf("load subscriptions into new %s: %w", item.name, err)
	}
	var markers []protocol.EventProgress
	for _, ev := range confirmed {
		markers = append(markers, ev)
	}
	sort.Slice(markers, func(i, j int) bool {
		if markers[i].SubscriptionID != markers[j].SubscriptionID {
			return markers[i].SubscriptionID < markers[j].SubscriptionID
		}
		return markers[i].Position < markers[j].Position
	})
	if err := next.loadEvents(m.nextReqID(), markers, m.cfg.TaskTimeout); err != nil {
		return fmt.Errorf("load confirmed event markers into new %s: %w", item.name, err)
	}
	var mu sync.Mutex
	old.setEventSink(func(late []protocol.EventProgress) {
		var forward []protocol.EventProgress
		for _, ev := range late {
			if ev.Status != "confirmed" || stopped[ev.SubscriptionID] {
				continue
			}
			mu.Lock()
			if next.adoptConfirmedEvent(ev) {
				forward = append(forward, ev)
			}
			mu.Unlock()
		}
		if len(forward) > 0 {
			_ = next.loadEvents(m.nextReqID(), forward, m.cfg.TaskTimeout)
		}
	})
	return nil
}
