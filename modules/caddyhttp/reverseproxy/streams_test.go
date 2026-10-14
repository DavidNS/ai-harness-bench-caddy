package reverseproxy

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

// TestStreamRegistry runs the registry scenarios: live upstream counting,
// registration, and closing only the streams whose upstream is gone.
func TestStreamRegistry(t *testing.T) {
	t.Run("live counting", TestStreamRegistryLiveCounting)
	t.Run("add requires live upstream", TestStreamRegistryAddRequiresLiveUpstream)
	t.Run("close orphaned", TestStreamRegistryCloseOrphaned)
	t.Run("first error", TestStreamRegistryCloseOrphanedReportsFirstError)
	t.Run("concurrent", TestStreamRegistryConcurrent)
}

func TestStreamRegistryLiveCounting(t *testing.T) {
	r := newStreamRegistry()

	r.addLive([]string{"a:1", "b:2"})
	r.addLive([]string{"a:1"})
	if got := r.live["a:1"]; got != 2 {
		t.Fatalf("live[a:1] = %d, want 2", got)
	}

	// one of two handlers using the upstream went away
	r.removeLive([]string{"a:1"})
	if got := r.live["a:1"]; got != 1 {
		t.Fatalf("live[a:1] = %d, want 1", got)
	}

	// the last ones went away
	r.removeLive([]string{"a:1", "b:2"})
	if len(r.live) != 0 {
		t.Fatalf("live = %v, want empty", r.live)
	}

	// removing what is not live is harmless
	r.removeLive([]string{"c:3"})
	if len(r.live) != 0 {
		t.Fatalf("live = %v, want empty", r.live)
	}
}

func TestStreamRegistryAddRequiresLiveUpstream(t *testing.T) {
	r := newStreamRegistry()

	if remove, ok := r.add("a:1", func() error { return nil }); ok || remove != nil {
		t.Fatal("add() registered a stream to an upstream that is not live")
	}
	if got := r.count(); got != 0 {
		t.Fatalf("count() = %d, want 0", got)
	}

	r.addLive([]string{"a:1"})
	remove, ok := r.add("a:1", func() error { return nil })
	if !ok {
		t.Fatal("add() refused a stream to a live upstream")
	}
	if got := r.count(); got != 1 {
		t.Fatalf("count() = %d, want 1", got)
	}
	remove()
	if got := r.count(); got != 0 {
		t.Fatalf("count() after remove = %d, want 0", got)
	}
}

func TestStreamRegistryCloseOrphaned(t *testing.T) {
	r := newStreamRegistry()
	r.addLive([]string{"kept:1", "gone:2"})

	var keptClosed, goneClosed atomic.Int32
	if _, ok := r.add("kept:1", func() error { keptClosed.Add(1); return nil }); !ok {
		t.Fatal("add(kept) failed")
	}
	if _, ok := r.add("gone:2", func() error { goneClosed.Add(1); return nil }); !ok {
		t.Fatal("add(gone) failed")
	}
	if _, ok := r.add("gone:2", func() error { goneClosed.Add(1); return nil }); !ok {
		t.Fatal("add(gone) failed")
	}

	// nothing is orphaned yet
	if err := r.closeOrphaned(); err != nil {
		t.Fatalf("closeOrphaned() = %v", err)
	}
	if keptClosed.Load() != 0 || goneClosed.Load() != 0 {
		t.Fatal("closeOrphaned() closed streams whose upstream is still live")
	}

	// the new config no longer has gone:2
	r.removeLive([]string{"gone:2"})
	if err := r.closeOrphaned(); err != nil {
		t.Fatalf("closeOrphaned() = %v", err)
	}
	if got := goneClosed.Load(); got != 2 {
		t.Fatalf("streams to the removed upstream closed = %d, want 2", got)
	}
	if got := keptClosed.Load(); got != 0 {
		t.Fatalf("stream to the retained upstream closed = %d, want 0", got)
	}
	if got := r.count(); got != 1 {
		t.Fatalf("count() = %d, want 1", got)
	}

	// closing again must not close the same streams twice
	if err := r.closeOrphaned(); err != nil {
		t.Fatalf("closeOrphaned() = %v", err)
	}
	if got := goneClosed.Load(); got != 2 {
		t.Fatalf("streams closed twice: %d", got)
	}
}

func TestStreamRegistryCloseOrphanedReportsFirstError(t *testing.T) {
	r := newStreamRegistry()
	r.addLive([]string{"a:1"})

	first := errors.New("first")
	var closed atomic.Int32
	// both fail: the error of whichever runs first is kept, and all are closed
	for range 2 {
		if _, ok := r.add("a:1", func() error { closed.Add(1); return first }); !ok {
			t.Fatal("add() failed")
		}
	}
	r.removeLive([]string{"a:1"})

	if err := r.closeOrphaned(); !errors.Is(err, first) {
		t.Fatalf("closeOrphaned() = %v, want %v", err, first)
	}
	if got := closed.Load(); got != 2 {
		t.Fatalf("closed = %d, want 2 (an error must not stop the others)", got)
	}
}

// Streams of handlers with dynamic upstreams can't be matched to a static
// upstream; they stay while a dynamic handler is live.
func TestStreamRegistryDynamicStreams(t *testing.T) {
	r := newStreamRegistry()

	// no dynamic handler: refused, like any upstream that is not live
	if _, ok := r.addStream("dyn:1", true, func() error { return nil }); ok {
		t.Fatal("a dynamic stream was registered without a live dynamic handler")
	}

	r.addLive([]string{dynamicLiveKey})
	var closed, staticClosed atomic.Int32
	if _, ok := r.addStream("dyn:1", true, func() error { closed.Add(1); return nil }); !ok {
		t.Fatal("a dynamic stream was refused although a dynamic handler is live")
	}
	// a static stream to an address that is not live is not helped by the dynamic handler
	if _, ok := r.addStream("dyn:1", false, func() error { staticClosed.Add(1); return nil }); ok {
		t.Fatal("a static stream was registered to an upstream that is not live")
	}

	// a reload keeps a dynamic handler
	r.addLive([]string{dynamicLiveKey})
	r.removeLive([]string{dynamicLiveKey})
	if err := r.closeOrphaned(); err != nil || closed.Load() != 0 {
		t.Fatalf("closeOrphaned() = %v, closed = %d; want the dynamic stream kept", err, closed.Load())
	}

	// the last dynamic handler is gone
	r.removeLive([]string{dynamicLiveKey})
	if err := r.closeOrphaned(); err != nil || closed.Load() != 1 {
		t.Fatalf("closeOrphaned() = %v, closed = %d; want the dynamic stream closed", err, closed.Load())
	}
}

func TestStreamRegistryConcurrent(t *testing.T) {
	r := newStreamRegistry()
	r.addLive([]string{"a:1"})

	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			if remove, ok := r.add("a:1", func() error { return nil }); ok {
				remove()
			}
		})
	}
	wg.Go(func() {
		r.removeLive([]string{"a:1"})
		_ = r.closeOrphaned()
	})
	wg.Wait()

	if got := r.count(); got != 0 {
		t.Fatalf("count() = %d, want 0", got)
	}
}
