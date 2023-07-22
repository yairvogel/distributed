package state

import "encoding/json"

// Micro-operation (Mop) represents an operation in a transaction (type txn = []Mop)
// Mop can be of two types "r" and "append" (specified in 'op' field).
// Serialized (marshalled) mop has three fields - if op is 'r' then 'vals' field is used. if 'op' is 'append' then 'val' field is used
type Mop struct {
	op   string
	key  int
	val  int
	vals []int
}

func (m *Mop) UnmarshalJSON(data []byte) error {
	var raw []any
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	m.op = raw[0].(string)
	m.key = int(raw[1].(float64))
	switch m.op {
	case "r":
		if raw[2] == nil {
			m.vals = nil
			return nil
		}
		vals := raw[2].([]any)
		m.vals = make([]int, len(vals))
		for i, v := range vals {
			m.vals[i] = int(v.(float64))
		}
	case "append":
		m.val = int(raw[2].(float64))
	}

	return nil
}

func (m Mop) MarshalJSON() ([]byte, error) {
	raw := make([]any, 3)
	raw[0] = m.op
	raw[1] = m.key

	switch m.op {
	case "r":
		raw[2] = m.vals
	case "append":
		raw[2] = m.val
	}

	return json.Marshal(raw)
}
