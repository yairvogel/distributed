package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	messages := hashset[int]{}
	node := maelstrom.NewNode()

	msgId := 1

	node.Handle("broadcast", func(m maelstrom.Message) error {
		body, err := unmarshalBody(m)
		if err != nil {
			return err
		}

		message, err := strconv.Atoi(fmt.Sprint(body["message"]))
		if err != nil {
			return err
		}

		messages.Add(message)

		msgId += 1
		body = map[string]any{
			"msg_id": msgId,
			"type":   "broadcast_ok",
		}

		return node.Reply(m, body)
	})

	node.Handle("topology", func(m maelstrom.Message) error {
		body, err := unmarshalBody(m)
		if err != nil {
			return err
		}

		body = map[string]any{
			"type": "topology_ok",
		}

		return node.Reply(m, body)
	})

	node.Handle("read", func(m maelstrom.Message) error {
		msgId += 1
		body := map[string]any{
			"msg_id":   msgId,
			"type":     "read_ok",
			"messages": messages.Keys(),
		}

		return node.Reply(m, body)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}

func unmarshalBody(msg maelstrom.Message) (map[string]any, error) {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return nil, err
	}
	return body, nil
}

type unit struct{}

type hashset[I comparable] map[I]unit

func (h hashset[I]) Add(item I) {
	h[item] = unit{}
}

func (h hashset[I]) Keys() []I {
	keys := make([]I, len(h))

	i := 0
	for k := range h {
		keys[i] = k
		i++
	}

	return keys
}
