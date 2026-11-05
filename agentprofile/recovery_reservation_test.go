package agentprofile

import "testing"

func reservationIntent(binding AuthorityBinding, slot, request string) ActionIntent {
	body := []byte("exact durable command")
	return ActionIntent{Action: PendingAction{Key: slot, Generation: 1, Request: commandKey(request), CommandDigest: commandHash(body), InputFence: recoveryFence(), Owner: "exec", Status: ActionPending}, Method: "host/v1", ExactCommand: body, Authority: binding}
}

func TestActionRequestNamespaceCannotBeReservedByAnotherSlot(t *testing.T) {
	l, binding := recoveryFixture(t)
	first := reservationIntent(binding, "first", "reserved-request")
	recoveryControl(t, l, "reserve-first", ControlCommand{PutAction: &PutActionCommand{Intent: first, Authority: binding}})
	second := reservationIntent(binding, "second", "reserved-request")
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{PutAction: &PutActionCommand{Intent: second, Authority: binding}}}, ErrUniqueSlot)
}

func TestRebindCannotReuseAnyHistoricalActionRequest(t *testing.T) {
	l, binding := recoveryFixture(t)
	first := reservationIntent(binding, "first", "first-request")
	second := reservationIntent(binding, "second", "second-request")
	recoveryControl(t, l, "reserve-first", ControlCommand{PutAction: &PutActionCommand{Intent: first, Authority: binding}})
	recoveryControl(t, l, "reserve-second", ControlCommand{PutAction: &PutActionCommand{Intent: second, Authority: binding}})
	next := recoveryTakeover(t, l, binding)
	recoveryControl(t, l, "first-absent", ControlCommand{ResolveAction: &ResolveActionCommand{Resolution: ActionResolution{Key: first.Action.Key, Generation: 1, Outcome: "absent", ObservedFence: recoveryFence()}}})
	rebind := RebindActionCommand{Key: first.Action.Key, ExpectedGeneration: 1, NewRequest: second.Action.Request, Method: first.Method, ExactCommand: first.ExactCommand, CommandDigest: first.Action.CommandDigest, Authority: next, FenceEvidence: digest('a')}
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{RebindAction: &rebind}}, ErrUniqueSlot)
	// Reusing a key from this action's own older generation is equally unsafe.
	rebind.NewRequest = commandKey("fresh-request")
	recoveryControl(t, l, "first-rebound", ControlCommand{RebindAction: &rebind})
	next = recoveryTakeover(t, l, next)
	recoveryControl(t, l, "second-generation-absent", ControlCommand{ResolveAction: &ResolveActionCommand{Resolution: ActionResolution{Key: first.Action.Key, Generation: 2, Outcome: "absent", ObservedFence: recoveryFence()}}})
	rebind.ExpectedGeneration, rebind.NewRequest, rebind.Authority = 2, first.Action.Request, next
	assertRecoveryRejected(t, l, Command{Control: &ControlCommand{RebindAction: &rebind}}, ErrUniqueSlot)
}
