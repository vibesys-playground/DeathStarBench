package checker

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// durability builds a small fixture (d1 and d2 follow d0, d3 mentions d1),
// then restarts one pod of every deployment and flushes every cache in
// turn. After each event it requires the fixture's and the dataset's
// timelines unchanged on every replica, and that writes still work: a new
// post of d0 reaches its followers within the bound (after a flush, a new
// follower is added first, before anything reads the flushed state).
func (s *suite) durability(ctx context.Context) error {
	if s.config.Hook == nil {
		return fmt.Errorf("no restart hook")
	}
	var err error
	if s.d, err = s.w.newUsers(ctx, 4); err != nil {
		return err
	}
	d := s.d
	for _, follower := range d[1:3] {
		if err := s.w.follow(ctx, follower, d[0], true); err != nil {
			return err
		}
	}
	var acked time.Time
	for i := 0; i < 3; i++ {
		if _, acked, err = s.w.compose(ctx, d[0], composeOptions{url: i == 1}); err != nil {
			return err
		}
		if _, acked, err = s.w.compose(ctx, d[3], composeOptions{mentions: []*user{d[1]}}); err != nil {
			return err
		}
	}
	if err := s.w.settle(ctx, acked); err != nil {
		return err
	}
	if err := s.verify(ctx, "before any restart"); err != nil {
		return err
	}
	for _, deployment := range s.config.Deployments {
		s.config.Log("restarting one pod of %s", deployment)
		if err := s.config.Hook.Restart(ctx, deployment); err != nil {
			return fmt.Errorf("restart hook for %s: %w", deployment, err)
		}
		if err := s.waitGateway(ctx); err != nil {
			return err
		}
		event := "after restarting a pod of " + deployment
		if err := s.verify(ctx, event); err != nil {
			return err
		}
		if err := s.writeAfter(ctx, event, false); err != nil {
			return err
		}
	}
	for _, cache := range s.config.Caches {
		s.config.Log("flushing %s", cache)
		if err := s.config.Hook.Flush(ctx, cache); err != nil {
			return fmt.Errorf("flush hook for %s: %w", cache, err)
		}
		event := "after flushing " + cache
		if s.config.FollowAfterFlush {
			if err := s.writeAfter(ctx, event, true); err != nil {
				return err
			}
		}
		if err := s.verify(ctx, event); err != nil {
			return err
		}
		if err := s.writeAfter(ctx, event, false); err != nil {
			return err
		}
	}
	return nil
}

func (s *suite) verify(ctx context.Context, event string) error {
	for _, u := range s.d {
		for _, home := range []bool{false, true} {
			if err := s.w.expect(ctx, u, home); err != nil {
				return fmt.Errorf("%s: %w", event, err)
			}
		}
	}
	if data := s.config.Dataset; data != nil {
		for _, u := range s.datasetSample(8) {
			for _, home := range []bool{false, true} {
				if err := s.expectSeeded(ctx, data, u, home, 0, 2); err != nil {
					return fmt.Errorf("%s: %w", event, err)
				}
			}
		}
	}
	return nil
}

// writeAfter composes as d0 and requires the post in every follower's home
// timeline within the bound; with follow, a new user follows d0 first.
func (s *suite) writeAfter(ctx context.Context, event string, follow bool) error {
	d := s.d
	if follow {
		newcomer, err := s.w.newUser(ctx)
		if err != nil {
			return fmt.Errorf("%s: %w", event, err)
		}
		if err := s.w.follow(ctx, newcomer, d[0], true); err != nil {
			return fmt.Errorf("%s: %w", event, err)
		}
		newcomer.home = nil // posts before the follow may or may not appear
		defer func() { newcomer.home = nil }()
	}
	_, acked, err := s.w.compose(ctx, d[0], composeOptions{})
	if err != nil {
		return fmt.Errorf("%s: %w", event, err)
	}
	if err := s.w.settle(ctx, acked); err != nil {
		return err
	}
	for follower := range d[0].followers {
		if follower == d[1] || follower == d[2] {
			if err := s.w.expect(ctx, follower, true); err != nil {
				return fmt.Errorf("%s: %w", event, err)
			}
			continue
		}
		// A newcomer: the new post must be first.
		page, err := s.w.c.read(ctx, true, follower.id, 0, 1)
		if err != nil {
			return fmt.Errorf("%s: %w", event, err)
		}
		if len(page) != 1 || page[0].PostID != d[0].posts[0] {
			return fmt.Errorf("%s: home timeline of new follower %s starts with %s, want the post of %s just composed", event, follower.name, describeAll(page), d[0].name)
		}
	}
	return nil
}

// waitGateway waits until nginx answers a read again.
func (s *suite) waitGateway(ctx context.Context) error {
	deadline := time.Now().Add(2 * time.Minute)
	for {
		reply, err := s.w.c.readRaw(ctx, false, "1", "0", "1")
		if err == nil && reply.status == 200 {
			return nil
		}
		if time.Now().After(deadline) {
			detail := ""
			if err != nil {
				detail = err.Error()
			} else {
				detail = fmt.Sprintf("HTTP %d %s", reply.status, strings.TrimSpace(string(reply.body)))
			}
			return fmt.Errorf("gateway not answering after the restart: %s", detail)
		}
		if err := sleepUntil(ctx, time.Now().Add(500*time.Millisecond)); err != nil {
			return err
		}
	}
}
