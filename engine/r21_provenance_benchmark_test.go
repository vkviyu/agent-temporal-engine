package engine

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/annotation"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/knowledge"
	"github.com/vkviyu/agent-temporal-engine/model"
)

type r21ProjectionFixture struct {
	summaries []knowledge.Summary
	byID      map[model.ObjectID]knowledge.Summary
	nodes     []nodeProjection
	ids       int
	bytes     int
}

func r21ProjectionID(prefix string, n int) model.ObjectID {
	return model.ObjectID(fmt.Sprintf("%064x", uint64(n)+uint64(len(prefix))*1_000_000))
}

func r21ProjectionJSON(b *testing.B, value any) []byte {
	b.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		b.Fatal(err)
	}
	return raw
}

func newR21ProjectionFixture(b *testing.B, depth, fanout int) r21ProjectionFixture {
	b.Helper()
	if depth < 1 || fanout < 1 {
		b.Fatal("R21 fixture dimensions must be positive")
	}

	rootSource := r21ProjectionID("source", 1)
	summaries := make([]knowledge.Summary, depth)
	byID := make(map[model.ObjectID]knowledge.Summary, depth)
	for i := range summaries {
		id := r21ProjectionID("summary", i+1)
		sources := []knowledge.SummarySource{{
			Observation: r21ProjectionID("observation", i+1),
			Content:     rootSource,
			PayloadKind: "memory",
		}}
		if i > 0 {
			sources = append(sources, knowledge.SummarySource{
				Observation: r21ProjectionID("nested-observation", i),
				Content:     summaries[i-1].ID,
				PayloadKind: "summary",
			})
		}
		summaries[i] = knowledge.Summary{
			ID:       id,
			Replaces: r21ProjectionID("replacement", i),
			Generation: knowledge.SummaryGeneration{
				Input:  knowledge.RecordedContent{Object: r21ProjectionID("input", i+1)},
				Output: knowledge.RecordedContent{Object: r21ProjectionID("output", i+1)},
			},
			Validation: knowledge.SummaryValidation{
				Evidence: []knowledge.RecordedContent{
					{Object: r21ProjectionID("evidence", i+1)},
					{Object: rootSource},
				},
			},
			Sources: sources,
			Omissions: []knowledge.SummaryOmission{
				{Observation: r21ProjectionID("omitted", i+1)},
			},
		}
		byID[summaries[i].ID] = summaries[i]
	}

	nodes := make([]nodeProjection, 0, fanout*3)
	var totalBytes int
	for i := 0; i < fanout; i++ {
		id := r21ProjectionID("node", i+1)
		summary := summaries[i%len(summaries)]
		contextPacket := knowledge.ContextPacket{
			ID:      id,
			GoalRef: rootSource,
			Items: []knowledge.ContextItem{{
				Observation: r21ProjectionID("packet-observation", i+1),
				Content:     rootSource,
			}},
		}
		annotationRecord := annotation.Record{
			ID:     id,
			Target: model.NodeRef{Kind: model.NodeObject, Object: rootSource},
			Evidence: []model.NodeRef{
				{Kind: model.NodeObject, Object: summary.ID},
				{Kind: model.NodeObject, Object: rootSource},
			},
		}
		for _, item := range []struct {
			encoding string
			value    any
		}{
			{"knowledge/Summary/v1+json", summary},
			{"knowledge/ContextPacket/v1+json", contextPacket},
			{"annotation/Record/v1+json", annotationRecord},
		} {
			raw := r21ProjectionJSON(b, item.value)
			nodes = append(nodes, nodeProjection{detail: api.NodeDetail{
				Ref:      model.NodeRef{Kind: model.NodeObject, Object: id},
				Payload:  id,
				Data:     raw,
				Encoding: item.encoding,
				Sources: []model.NodeRef{
					{Kind: model.NodeObject, Object: rootSource},
					{Kind: model.NodeObject, Object: summary.ID},
				},
			}})
			totalBytes += len(raw)
		}
	}
	return r21ProjectionFixture{
		summaries: summaries, byID: byID,
		nodes: nodes,
		ids:   depth + fanout*3,
		bytes: totalBytes,
	}
}

var (
	r21SummarySink []policyDependency
	r21NodeSink    []policyDependency
	r21ContextSink api.NodeContext
)

func BenchmarkR21SummaryProvenanceProjection(b *testing.B) {
	for _, spec := range []struct {
		depth  int
		fanout int
	}{
		{depth: 8, fanout: 32},
		{depth: 32, fanout: 256},
		{depth: 128, fanout: 1024},
	} {
		b.Run(fmt.Sprintf("depth=%d/fanout=%d", spec.depth, spec.fanout), func(b *testing.B) {
			fixture := newR21ProjectionFixture(b, spec.depth, spec.fanout)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var dependencies []policyDependency
				for _, summary := range fixture.summaries {
					dependencies = append(dependencies, summaryPolicyDependencies(summary)...)
					for _, source := range summaryParents(summary) {
						if parent, found := fixture.byID[source]; found {
							dependencies = append(dependencies, summaryPolicyDependencies(parent)...)
						}
					}
				}
				r21SummarySink = dependencies
			}
			b.StopTimer()
			b.ReportMetric(float64(fixture.ids), "fixture-ids")
			b.ReportMetric(float64(fixture.bytes), "fixture-json-B")
			b.ReportMetric(float64(len(r21SummarySink)), "dependencies/op")
		})
	}
}

func BenchmarkR21NodeDisclosureJSONDecode(b *testing.B) {
	for _, spec := range []struct {
		depth  int
		fanout int
	}{
		{depth: 8, fanout: 32},
		{depth: 32, fanout: 256},
		{depth: 128, fanout: 1024},
	} {
		b.Run(fmt.Sprintf("depth=%d/fanout=%d", spec.depth, spec.fanout), func(b *testing.B) {
			fixture := newR21ProjectionFixture(b, spec.depth, spec.fanout)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dependencies := make([]policyDependency, 0, len(fixture.nodes)*3)
				for _, node := range fixture.nodes {
					dependencies = append(dependencies, nodeDisclosureDependencies(node)...)
				}
				r21NodeSink = dependencies
			}
			b.StopTimer()
			b.ReportMetric(float64(fixture.ids), "fixture-ids")
			b.ReportMetric(float64(fixture.bytes), "fixture-json-B")
			b.ReportMetric(float64(len(r21NodeSink)), "dependencies/op")
		})
	}
}

// This projection benchmark intentionally exercises the same node payload
// shapes that the Engine output gate decodes. It does not authorize access and
// must never be used as evidence that a policy result can be cached.
func BenchmarkR21DisclosurePayloadShapes(b *testing.B) {
	fixture := newR21ProjectionFixture(b, 32, 256)
	for _, encoding := range []string{
		"knowledge/Summary/v1+json",
		"knowledge/ContextPacket/v1+json",
		"annotation/Record/v1+json",
	} {
		b.Run(strings.TrimSuffix(encoding, "/v1+json"), func(b *testing.B) {
			nodes := make([]nodeProjection, 0, len(fixture.nodes)/3)
			for _, node := range fixture.nodes {
				if node.detail.Encoding == encoding {
					nodes = append(nodes, node)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var dependencies []policyDependency
				for _, node := range nodes {
					dependencies = append(dependencies, nodeDisclosureDependencies(node)...)
				}
				r21NodeSink = dependencies
			}
			b.StopTimer()
			b.ReportMetric(float64(len(nodes)), "nodes")
			b.ReportMetric(float64(len(r21NodeSink)), "dependencies/op")
		})
	}
}
