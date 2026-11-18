package observation

import (
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
)

func TestR13DisabledAndCapacity(t *testing.T) {
	for _, c := range []*Collector{nil, {}} {
		var dst [1]Event
		if n := testing.AllocsPerRun(100, func() {
			c.Emit(Event{Kind: Commit})
			c.Finish(c.Start(), Event{Kind: Commit})
			c.Drain(dst[:])
		}); n != 0 {
			t.Fatalf("disabled allocated %v", n)
		}
		if c.Enabled() || c.Drain(dst[:]) != 0 {
			t.Fatal("disabled collector delivered")
		}
		if a, b := c.Drops(); a != 0 || b != 0 {
			t.Fatal(a, b)
		}
	}
	if n := testing.AllocsPerRun(100, func() {
		c, err := New(0)
		if c != nil || err != nil {
			t.Fatal(c, err)
		}
	}); n != 0 {
		t.Fatal(n)
	}
	for _, size := range []int{-1, MaxCapacity + 1} {
		if c, err := New(size); c != nil || err != ErrCapacity {
			t.Fatal(c, err)
		}
	}
	c, err := New(MaxCapacity)
	if err != nil || len(c.events) != MaxCapacity {
		t.Fatal(err)
	}
}

func TestR13OverflowContentionAndFIFO(t *testing.T) {
	c, _ := New(2)
	c.Emit(Event{Kind: Prepare, Count: 1})
	c.Emit(Event{Kind: P1, Count: 2})
	c.Emit(Event{Kind: Commit, Count: 3})
	c.mu.Lock()
	done := make(chan struct{})
	go func() { c.Emit(Event{Kind: Close}); close(done) }()
	// This completion must not require releasing the consumer's lock.
	<-done
	c.mu.Unlock()
	if full, contention := c.Drops(); full != 1 || contention != 1 {
		t.Fatal(full, contention)
	}
	var dst [1]Event
	if c.Drain(dst[:]) != 1 || dst[0].Count != 1 {
		t.Fatal(dst)
	}
	c.Emit(Event{Kind: Commit, Count: 4}) // wraps the bounded ring
	if c.Drain(dst[:]) != 1 || dst[0].Count != 2 {
		t.Fatal(dst)
	}
	if c.Drain(dst[:]) != 1 || dst[0].Count != 4 {
		t.Fatal(dst)
	}
	if c.Drain(dst[:]) != 0 {
		t.Fatal("duplicate")
	}
}

func TestR13ConcurrentProducersAndDrains(t *testing.T) {
	c, _ := New(31)
	var producers, consumers sync.WaitGroup
	var finished atomic.Bool
	var consumed atomic.Uint64
	var seen sync.Map
	for i := 0; i < 3; i++ {
		consumers.Add(1)
		go func() {
			defer consumers.Done()
			var dst [7]Event
			for {
				n := c.Drain(dst[:])
				for _, e := range dst[:n] {
					if _, duplicate := seen.LoadOrStore(e.Count, true); duplicate {
						t.Error("duplicate event", e.Count)
					}
				}
				consumed.Add(uint64(n))
				if n == 0 && finished.Load() {
					return
				}
			}
		}()
	}
	for p := 0; p < 4; p++ {
		producers.Add(1)
		go func(p int) {
			defer producers.Done()
			for i := 0; i < 2000; i++ {
				c.Emit(Event{Kind: Commit, Count: uint64(p*2000 + i)})
			}
		}(p)
	}
	producers.Wait()
	finished.Store(true)
	consumers.Wait()
	full, contention := c.Drops()
	if consumed.Load()+full+contention != 8000 {
		t.Fatal(consumed.Load(), full, contention)
	}
}

func TestR13EventHasOnlyNumericFields(t *testing.T) {
	typ := reflect.TypeOf(Event{})
	for i := 0; i < typ.NumField(); i++ {
		switch typ.Field(i).Type.Kind() {
		case reflect.Uint8, reflect.Uint64, reflect.Int64, reflect.Bool:
		default:
			t.Fatalf("event can carry content: %v", typ.Field(i))
		}
	}
}

func BenchmarkR13Mailbox(b *testing.B) {
	for _, mode := range []string{"disabled", "enabled", "full"} {
		b.Run(mode, func(b *testing.B) {
			var c *Collector
			if mode != "disabled" {
				c, _ = New(64)
			}
			if mode == "full" {
				for i := 0; i < 64; i++ {
					c.Emit(Event{})
				}
			}
			var dst [64]Event
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := c.Start()
				c.Finish(start, Event{Kind: P1, Count: 1, Bytes: 1024})
				if mode == "enabled" && i%64 == 63 {
					c.Drain(dst[:])
				}
			}
		})
	}
}
