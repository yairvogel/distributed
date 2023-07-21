package main

import (
	"distributed/broadcast/hashset"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type context struct {
	node           *maelstrom.Node
	messages       *hashset.Hashset[int]
	connectedNodes []string
}

type TopologyMessageBody struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

type GossipMessageBody struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

func main() {
	messages := hashset.New[int]()
	node := maelstrom.NewNode()
	context := context{node: node, messages: messages}

	context.handle("topology", handleTopology)
	context.handle("broadcast", handleBroadcast)
	context.handle("gossip", handleGossip)
	context.handle("gossip_ok", handleGossipOk)
	context.handle("read", handleRead)

	go tick(&context)

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}

func (c *context) handle(typ string, handler func(c_ *context, m maelstrom.Message) error) {
	c.node.Handle(typ, func(m_ maelstrom.Message) error { return handler(c, m_) })
}

func handleTopology(c *context, m maelstrom.Message) error {
	var body TopologyMessageBody
	if err := json.Unmarshal(m.Body, &body); err != nil {
		return err
	}

	c.connectedNodes = shallowCopy(body.Topology[c.node.ID()])

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

	c.messages.Add(message)

	return c.node.Reply(m, map[string]any{"type": "broadcast_ok"})
}

func handleGossip(c *context, m maelstrom.Message) error {
	var body GossipMessageBody
	if err := json.Unmarshal(m.Body, &body); err != nil {
		return err
	}

	responseMessages := c.messages.Difference(body.Messages)
	c.messages.Union(body.Messages)

	if len(responseMessages) > 0 {
		c.node.Reply(m, GossipMessageBody{Type: "gossip_ok", Messages: responseMessages})
	}

	return nil
}

func handleGossipOk(c *context, m maelstrom.Message) error {
	var body GossipMessageBody
	if err := json.Unmarshal(m.Body, &body); err != nil {
		return err
	}

	c.messages.Union(body.Messages)
	return nil
}

func handleRead(c *context, m maelstrom.Message) error {
	keys := c.messages.Items()

	body := map[string]any{
		"type":     "read_ok",
		"messages": keys,
	}

	return c.node.Reply(m, body)
}

func tick(c *context) {
	for {
		body := GossipMessageBody{Type: "gossip", Messages: c.messages.Items()}
		for _, c_node := range c.connectedNodes {
			c.node.Send(c_node, body)
		}

		time.Sleep(100 * time.Millisecond)
	}
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
