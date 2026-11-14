package agentprofile

import (
	"bytes"
	"reflect"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func lifecycleSelected(c LifecycleCommand) int {
	n := 0
	if c.PutQuery != nil {
		n++
	}
	if c.PutActivity != nil {
		n++
	}
	if c.PutAttempt != nil {
		n++
	}
	if c.PutResult != nil {
		n++
	}
	if c.PutMessage != nil {
		n++
	}
	if c.ConsumeMessage != nil {
		n++
	}
	if c.PutContinuation != nil {
		n++
	}
	return n
}
func validLife(s string) bool { return commandText(s) }
func validateLifecycleCommand(c LifecycleCommand) error {
	if lifecycleSelected(c) != 1 {
		return ErrCommandShape
	}
	if q := c.PutQuery; q != nil {
		if !validLife(q.ID) || !validLife(string(q.Space)) || !validLife(string(q.Owner)) || q.Version != 1 || q.Status != "open" || len(q.Answer) != 0 || len(q.Input) > MaxCommandBytes {
			return ErrCommandShape
		}
	}
	if a := c.PutActivity; a != nil {
		if !validLife(a.ID) || !validLife(string(a.Execution)) || !validLife(a.Kind) || a.Version == 0 || a.Status == "" {
			return ErrCommandShape
		}
	}
	if a := c.PutAttempt; a != nil {
		if !validLife(a.ID) || !validLife(a.Activity) || a.Sequence == 0 || a.Incarnation == 0 || a.Status == "" {
			return ErrCommandShape
		}
	}
	if r := c.PutResult; r != nil {
		if !validLife(r.ID) || !validLife(r.Activity) || !validLife(r.Attempt) || r.Incarnation == 0 || r.Outcome == "" || len(r.Payload) > MaxCommandBytes || !validResultCapture(*r) {
			return ErrCommandShape
		}
	}
	if m := c.PutMessage; m != nil {
		if !validLife(m.ID) || !validLife(string(m.Sender)) || !validLife(string(m.Recipient)) || m.GoalEpoch == 0 || len(m.Payload) > MaxCommandBytes {
			return ErrCommandShape
		}
	}
	if m := c.ConsumeMessage; m != nil {
		if !validLife(m.Message) || !validLife(string(m.Recipient)) || m.Incarnation == 0 || m.GoalEpoch == 0 {
			return ErrCommandShape
		}
	}
	if x := c.PutContinuation; x != nil {
		if !validLife(string(x.Execution)) || x.Version == 0 || x.Incarnation == 0 || !validLife(x.PendingKey) || x.Phase == "" || validateContinuationPosition(*x) != nil {
			return ErrCommandShape
		}
	}
	return nil
}
func lifeText(b []byte, s string) { b2, e := codec.AppendString(b, s); _ = e; _ = b2 }
func encLifecycle(c LifecycleCommand) ([]byte, error) {
	if err := validateLifecycleCommand(c); err != nil {
		return nil, err
	}
	op := uint64(0)
	switch {
	case c.PutQuery != nil:
		op = 1
	case c.PutActivity != nil:
		op = 2
	case c.PutAttempt != nil:
		op = 3
	case c.PutResult != nil:
		op = 4
	case c.PutMessage != nil:
		op = 5
	case c.ConsumeMessage != nil:
		op = 6
	case c.PutContinuation != nil:
		op = 8
	}
	b := codec.AppendUint(nil, op)
	put := func(s string) { b, _ = codec.AppendString(b, s) }
	bytesPut := func(v []byte) { b, _ = codec.AppendBytes(b, v) }
	switch op {
	case 1:
		q := c.PutQuery
		put(q.ID)
		put(q.Session)
		put(string(q.Space))
		put(string(q.Owner))
		bytesPut(q.Input)
		b, _ = appendFence(b, q.InputFence)
		put(q.Status)
		b = codec.AppendUint(b, q.Version)
		bytesPut(q.Answer)
	case 2:
		a := c.PutActivity
		put(a.ID)
		put(string(a.Execution))
		put(a.Kind)
		put(a.Invocation)
		b = codec.AppendUint(b, a.Step)
		b, _ = appendFence(b, a.InputFence)
		put(a.Status)
		b = codec.AppendUint(b, a.Version)
	case 3:
		a := c.PutAttempt
		put(a.ID)
		put(a.Activity)
		b = codec.AppendUint(b, a.Sequence)
		b = codec.AppendUint(b, a.Incarnation)
		put(a.Status)
	case 4:
		r := c.PutResult
		put(r.ID)
		put(r.Activity)
		put(r.Attempt)
		put(string(r.RequestDigest))
		b = codec.AppendUint(b, r.Incarnation)
		put(r.Outcome)
		bytesPut(r.Payload)
		put(r.Capture)
		put(r.Response)
		b = codec.AppendUint(b, uint64(len(r.OutputChunks)))
		for _, id := range r.OutputChunks {
			put(string(id))
		}
	case 5:
		m := c.PutMessage
		put(m.ID)
		put(string(m.Sender))
		put(string(m.Recipient))
		bytesPut(m.Payload)
		b = codec.AppendUint(b, m.GoalEpoch)
	case 6:
		m := c.ConsumeMessage
		put(m.Message)
		put(string(m.Recipient))
		b = codec.AppendUint(b, m.Incarnation)
		b = codec.AppendUint(b, m.GoalEpoch)
	case 8:
		return appendControlValue(b, reflect.ValueOf(*c.PutContinuation), 0)
	}
	return b, nil
}
func decodeLifecycleCommand(raw []byte) (*LifecycleCommand, error) {
	d := codec.NewDecoder(raw)
	op, e := d.Uint()
	if e != nil {
		return nil, e
	}
	c := &LifecycleCommand{}
	str := func() (string, error) { return d.String(1024) }
	by := func() ([]byte, error) { return d.Bytes(MaxCommandBytes) }
	switch op {
	case 1:
		q := Query{}
		var s string
		if q.ID, e = str(); e == nil {
			q.Session, e = str()
		}
		if e == nil {
			s, e = str()
			q.Space = model.SpaceID(s)
		}
		if e == nil {
			s, e = str()
			q.Owner = model.PrincipalID(s)
		}
		if e == nil {
			q.Input, e = by()
		}
		if e == nil {
			q.InputFence, e = readFence(d)
		}
		if e == nil {
			q.Status, e = str()
		}
		if e == nil {
			q.Version, e = d.Uint()
		}
		if e == nil {
			q.Answer, e = by()
		}
		c.PutQuery = &q
	case 2:
		a := Activity{}
		var s string
		if a.ID, e = str(); e == nil {
			s, e = str()
			a.Execution = model.ExecutionID(s)
		}
		if e == nil {
			a.Kind, e = str()
		}
		if e == nil {
			a.Invocation, e = str()
		}
		if e == nil {
			a.Step, e = d.Uint()
		}
		if e == nil {
			a.InputFence, e = readFence(d)
		}
		if e == nil {
			a.Status, e = str()
		}
		if e == nil {
			a.Version, e = d.Uint()
		}
		c.PutActivity = &a
	case 3:
		a := Attempt{}
		if a.ID, e = str(); e == nil {
			a.Activity, e = str()
		}
		if e == nil {
			a.Sequence, e = d.Uint()
		}
		if e == nil {
			a.Incarnation, e = d.Uint()
		}
		if e == nil {
			a.Status, e = str()
		}
		c.PutAttempt = &a
	case 4:
		r := ActivityResult{}
		var s string
		if r.ID, e = str(); e == nil {
			r.Activity, e = str()
		}
		if e == nil {
			r.Attempt, e = str()
		}
		if e == nil {
			s, e = str()
			r.RequestDigest = model.ObjectID(s)
		}
		if e == nil {
			r.Incarnation, e = d.Uint()
		}
		if e == nil {
			r.Outcome, e = str()
		}
		if e == nil {
			r.Payload, e = by()
		}
		if e == nil {
			r.Capture, e = str()
		}
		if e == nil {
			r.Response, e = str()
		}
		if e == nil {
			var count uint64
			count, e = d.Uint()
			if count > MaxLifecycleRecords {
				return nil, ErrCommandShape
			}
			for i := uint64(0); e == nil && i < count; i++ {
				var id string
				id, e = str()
				r.OutputChunks = append(r.OutputChunks, model.ObjectID(id))
			}
		}
		c.PutResult = &r
	case 5:
		m := Message{}
		var s string
		if m.ID, e = str(); e == nil {
			s, e = str()
			m.Sender = model.ExecutionID(s)
		}
		if e == nil {
			s, e = str()
			m.Recipient = model.ExecutionID(s)
		}
		if e == nil {
			m.Payload, e = by()
		}
		if e == nil {
			m.GoalEpoch, e = d.Uint()
		}
		c.PutMessage = &m
	case 6:
		m := MessageConsumption{}
		var s string
		if m.Message, e = str(); e == nil {
			s, e = str()
			m.Recipient = model.ExecutionID(s)
		}
		if e == nil {
			m.Incarnation, e = d.Uint()
		}
		if e == nil {
			m.GoalEpoch, e = d.Uint()
		}
		c.ConsumeMessage = &m
	case 8:
		x := Continuation{}
		e = readControlValue(d, reflect.ValueOf(&x).Elem(), 0)
		if e == nil && x.Position == nil {
			return nil, ErrCommandShape
		}
		c.PutContinuation = &x
	default:
		return nil, ErrCommandVersion
	}
	if e == nil {
		e = d.Done()
	}
	if e != nil {
		return nil, e
	}
	if e = validateLifecycleCommand(*c); e != nil {
		return nil, e
	}
	canonical, e := encLifecycle(*c)
	if e != nil || !bytes.Equal(canonical, raw) {
		return nil, ErrCommandShape
	}
	return c, nil
}

func encodeLifecycleCommand(c LifecycleCommand) ([]byte, error) { return encLifecycle(c) }
