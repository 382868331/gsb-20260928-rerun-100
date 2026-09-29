package extensions

import (
	"fmt"
	"sort"
)

type Requirement struct {
	Extension  string
	MinVersion string
	Capability string
}

type Manifest struct {
	Name         string
	Version      string
	Builtin      string
	Capabilities []string
	Requires     []Requirement
}

func compareVersion(a, b string) int {
	pa := parseVersion(a)
	pb := parseVersion(b)
	for i := 0; i < 3; i++ {
		if pa[i] != pb[i] {
			if pa[i] < pb[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}

func parseVersion(v string) [3]int {
	var out [3]int
	num := 0
	part := 0
	for _, r := range v {
		if r >= '0' && r <= '9' {
			num = num*10 + int(r-'0')
			continue
		}
		if r == '.' && part < 2 {
			out[part] = num
			part++
			num = 0
		} else {
			break
		}
	}
	out[part] = num
	return out
}

func startupOrder(manifests []Manifest) ([]Manifest, error) {
	byName := map[string]Manifest{}
	for _, m := range manifests {
		if _, exists := byName[m.Name]; exists {
			return nil, fmt.Errorf("duplicate extension %q", m.Name)
		}
		byName[m.Name] = m
	}

	for _, m := range manifests {
		for _, req := range m.Requires {
			dep, ok := byName[req.Extension]
			if !ok {
				return nil, fmt.Errorf("extension %q requires missing extension %q", m.Name, req.Extension)
			}
			if req.MinVersion != "" && compareVersion(dep.Version, req.MinVersion) < 0 {
				return nil, fmt.Errorf("extension %q requires %s >= %s, found %s", m.Name, req.Extension, req.MinVersion, dep.Version)
			}
			if req.Capability != "" && !contains(dep.Capabilities, req.Capability) {
				return nil, fmt.Errorf("extension %q requires capability %q from %q", m.Name, req.Capability, req.Extension)
			}
		}
	}

	state := map[string]int{}
	var order []Manifest
	var visit func(string, []string) error
	visit = func(name string, stack []string) error {
		if state[name] == 1 {
			return fmt.Errorf("dependency cycle: %v", append(append([]string{}, stack...), name))
		}
		if state[name] == 2 {
			return nil
		}
		state[name] = 1
		m := byName[name]
		deps := append([]Requirement{}, m.Requires...)
		sort.Slice(deps, func(i, j int) bool { return deps[i].Extension < deps[j].Extension })
		for _, dep := range deps {
			if err := visit(dep.Extension, append(stack, name)); err != nil {
				return err
			}
		}
		state[name] = 2
		order = append(order, m)
		return nil
	}
	names := make([]string, 0, len(manifests))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := visit(name, nil); err != nil {
			return nil, err
		}
	}
	return order, nil
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
