package agentprofile

// ValidateStateSnapshot validates the API snapshot against the record-backed
// state format, including record budgets and cross-record relationships.
func ValidateStateSnapshot(snapshot Snapshot) error {
	_, err := EncodeRecordImage(snapshot)
	return err
}

// RestoreStateSnapshot validates and normalizes an API snapshot and returns an
// independently owned Ledger using the record-backed state rules.
func RestoreStateSnapshot(snapshot Snapshot) (*Ledger, error) {
	image, err := EncodeRecordImage(snapshot)
	if err != nil {
		return nil, err
	}
	return image.Restore()
}
