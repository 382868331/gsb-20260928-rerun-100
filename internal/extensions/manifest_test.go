package extensions

import "testing"

func TestStartupOrderRespectsDependencies(t *testing.T) {
	order, err := startupOrder(BuiltinManifests())
	if err != nil {
		t.Fatal(err)
	}
	if len(order) != 2 || order[0].Builtin != "echo" || order[1].Builtin != "calculator" {
		t.Fatalf("expected echo then calculator, got %#v", order)
	}
}

func TestStartupCheckRejectsMissingVersionAndCapability(t *testing.T) {
	manifests := []Manifest{
		{Name: "base", Version: "1.0.0", Capabilities: []string{"a"}},
		{
			Name:         "old",
			Version:      "0.9.0",
			Capabilities: []string{"b"},
			Requires: []Requirement{{
				Extension:  "base",
				MinVersion: "2.0.0",
			}},
		},
	}
	if _, err := startupOrder(manifests); err == nil {
		t.Fatal("expected minimum-version incompatibility")
	}

	manifests[1].Requires[0].MinVersion = "1.0.0"
	manifests[1].Requires[0].Capability = "missing"
	if _, err := startupOrder(manifests); err == nil {
		t.Fatal("expected missing-capability incompatibility")
	}
}

func TestStartupCheckRejectsCycles(t *testing.T) {
	manifests := []Manifest{
		{Name: "a", Version: "1.0.0", Requires: []Requirement{{Extension: "b"}}},
		{Name: "b", Version: "1.0.0", Requires: []Requirement{{Extension: "a"}}},
	}
	if _, err := startupOrder(manifests); err == nil {
		t.Fatal("expected dependency cycle error")
	}
}
