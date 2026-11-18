package reference

// RootSlot identifies a logical publication root. The model deliberately
// tracks slots and versions instead of production object IDs.
type RootSlot uint8

const (
	DomainRoot RootSlot = iota
	KnowledgeRoot
	BranchRoot
	ControlRoot
	ReceiptRoot
)

type RootProjection struct {
	Fence    uint64
	Versions [5]uint64
	Receipts map[string]uint64
}

func NewRootProjection() *RootProjection {
	return &RootProjection{Receipts: map[string]uint64{}}
}

func (p *RootProjection) Accept(request string, changed ...RootSlot) uint64 {
	p.Fence++
	p.Receipts[request] = p.Fence
	p.Versions[ReceiptRoot]++
	for _, slot := range changed {
		p.Versions[slot]++
	}
	return p.Fence
}

func (p *RootProjection) Reject() {}

func (p *RootProjection) Retry(request string) uint64 {
	return p.Receipts[request]
}
