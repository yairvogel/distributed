package state

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type txn = []Mop

type key = int
type vals = []int
type State struct {
	values map[key]vals
}

type LinKvResponseBody struct {
	State map[key]vals `json:"value"`
}

func NewState() *State {
	return &State{make(map[key]vals)}
}

func (s State) Clone() *State {
	c := NewState()
	for k, v := range s.values {
		c.values[k] = v
	}
	return c
}

func Transact(t txn, orig *State, n *maelstrom.Node) (txn, *State) {
	var t2 txn = make(txn, len(t))
	s := orig.Clone()
	for i, m := range t {
		switch m.op {
		case "r":
			values := s.getOrDefault(m.key)
			t2[i] = Mop{op: m.op, key: m.key, val: m.val, vals: values}
		case "append":
			values := s.getOrDefault(m.key)
			values = append(values, m.val)

			s.values[m.key] = values
			t2[i] = m
		}
	}

	return t2, s
}

func LoadState(n *maelstrom.Node) *State {
	msg, err := syncRPC(n, "lin-kv", map[string]any{
		"type": "read",
		"key":  "root",
	})

	if err != nil {
		log.Printf("%v\n", err)
		return NewState()
	}

	var body LinKvResponseBody
	if err = json.Unmarshal(msg.Body, &body); err != nil {
		log.Printf("%v\n", err)
		return NewState()
	}

	state := State{body.State}
	return &state
}

func SaveState(from *State, to *State, n *maelstrom.Node) error {
	m, err := syncRPC(n, "lin-kv", map[string]any{
		"type":                 "cas",
		"key":                  "root",
		"from":                 from.values,
		"to":                   to.values,
		"create_if_not_exists": true,
	})

	if err != nil {
		return err
	}

	var body map[string]any
	if err = json.Unmarshal(m.Body, &body); err != nil {
		return err
	}

	if body["type"] == "error" {
		return fmt.Errorf("Conflict: transaction has a conflict with another transaction")
	}

	return nil
}

func (s State) getOrDefault(k key) vals {
	if val, hasKey := s.values[k]; hasKey {
		return val
	}

	return vals{}
}

func syncRPC(node *maelstrom.Node, dst string, body any) (maelstrom.Message, error) {
	var timeout time.Duration = 1000 * time.Millisecond
	resChan := make(chan maelstrom.Message)

	node.RPC(dst, body, func(m maelstrom.Message) error {
		resChan <- m
		return nil
	})

	select {
	case res := <-resChan:
		return res, nil
	case <-time.After(timeout):
		return maelstrom.Message{}, fmt.Errorf("RPC timeout")
	}
}
