package main

import (
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()

	msgId := 1

	node.Handle("generate", func(msg maelstrom.Message) error {
		body, err := unmarshalBody(msg)
		if err != nil {
			return err
		}

		body["type"] = "generate_ok"
		msgId += 1
		body["msg_id"] = msgId
		body["id"] = node.ID() + fmt.Sprint(msgId)

		return node.Reply(msg, body)
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
