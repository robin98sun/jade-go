package kernel

import (
	ds "uta.edu/aces/jadesdk/data_structure"
)

// Requirements the capabilities and resources requirements
type Requirements struct {
	Collective  []*ds.Capability      `json:"collective,omitempty"`
	Exclusive   []*ds.Capability      `json:"exclusive,omitempty"`
	Allocations map[string]*AllocationUnit `json:"allocations,omitempty"`
}

func (r *Requirements) GetQueryKey() string {
	query_key := ""
	if len(r.Collective) > 0 {
		query_key += "#collective:"
		for _, c := range r.Collective {
			query_key += "&"+c.GetKey()
		}
	}
	if len(r.Exclusive) > 0 {
		query_key += "#exclusive:"
		for _, c := range r.Exclusive {
			query_key += "&"+c.GetKey()
		}
	}
	return query_key
}


func (r *Requirements) GetModule(moduleName string) *AllocationUnit {
	if r == nil || moduleName == "" {
		return nil
	}
	if m, exists := r.Allocations[moduleName]; exists {
		return m
	}
	return nil
}

func (r *Requirements) valid() bool {
	if len(r.Collective) == 0 && len(r.Exclusive) == 0 {
		return false
	}
	if len(r.Allocations) == 0 {
		return false
	}
	valid := true
	for _, m := range r.Allocations {
		if !m.Valid() {
			valid = false
			break
		}
	}
	return valid
}
