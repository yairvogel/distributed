package main

import (
	"distributed/broadcast/hashset"
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type context struct {
	node           *maelstrom.Node
	messages       *hashset.Hashset[int]
	connectedNodes []string
}

func main() {
	messages := hashset.New[int]()
	node := maelstrom.NewNode()
	context := context{node: node, messages: &messages}

	context.handle("topology", handleTopology)
	context.handle("broadcast", handleBroadcast)
	context.handle("gossip", handleGossip)
	context.handle("read", handleRead)

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}

func (c *context) handle(typ string, handler func(c_ *context, m maelstrom.Message) error) {
	c.node.Handle(typ, func(m_ maelstrom.Message) error { return handler(c, m_) })
}

type TopologyMessageBody struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

func handleTopology(c *context, m maelstrom.Message) error {
	var body TopologyMessageBody
	if err := json.Unmarshal(m.Body, &body); err != nil {
		return err
	}

	c.connectedNodes = shallowCopy(body.Topology[c.node.ID()])
	log.Printf("connectedNodes: %v\n", c.connectedNodes)

	return c.node.Reply(m, map[string]any{"type": "topology_ok"})
}

func handleBroadcast(c *context, m maelstrom.Message) error {
	body, err := unmarshalBody(m)
	if err != nil {
		return err
	}

	message, err := strconv.Atoi(fmt.Sprint(body["message"]))
	if err != nil {
		return err
	}

	body = map[string]any{
		"type":    "gossip",
		"message": message,
	}
	log.Printf("gossip body from broadcast: %v\n", body)
	log.Printf("connectedNodes: %v", c.connectedNodes)

	c.messages.Add(message)
	for _, c_node := range c.connectedNodes {
		log.Printf("sending message from broadcast to %v\n", c_node)
		c.node.Send(c_node, body)
	}

	body = map[string]any{
		"type": "broadcast_ok",
	}

	return c.node.Reply(m, body)
}

func handleGossip(c *context, m maelstrom.Message) error {
	body, err := unmarshalBody(m)
	if err != nil {
		return err
	}

	message, err := strconv.Atoi(fmt.Sprint(body["message"]))
	if err != nil {
		return err
	}

	if !c.messages.TryAdd(message) {
		return nil
	}

	for _, c_node := range c.connectedNodes {
		if c_node == m.Src {
			continue
		}

		err := c.node.Send(c_node, body)
		if err != nil {
			return err
		}
	}

	return nil
}

func handleRead(c *context, m maelstrom.Message) error {
	keys := c.messages.Keys()

	body := map[string]any{
		"type":     "read_ok",
		"messages": keys,
	}

	return c.node.Reply(m, body)
}

func unmarshalBody(msg maelstrom.Message) (map[string]any, error) {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return nil, err
	}
	return body, nil
}

func shallowCopy(src []string) []string {
	dst := make([]string, len(src))
	copy(dst, src)
	return dst
}
