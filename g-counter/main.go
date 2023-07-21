package main

import (
	"distributed/g-counter/counter"
	"encoding/json"
	"log"
	"math/rand"
	"strconv"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type AddMessageBody struct {
	Type  string `json:"type"`
	Delta uint   `json:"delta"`
}

type MergeMessageBody struct {
	Type      string `json:"type"`
	State     []uint `json:"state"`
	MsgId     int    `json:"msg_id"`
	InReplyTo int    `json:"in_reply_to,omitempty"`
}

type Context struct {
	Node    *maelstrom.Node
	Counter *counter.Counter
	MsgId   int
}

func main() {
	node := maelstrom.NewNode()
	context := Context{
		Node:  node,
		MsgId: 1,
	}

	context.Handle("init", handleInit)
	context.Handle("read", handleRead)
	context.Handle("add", handleAdd)
	context.Handle("merge", handleMerge)
	context.Handle("merge_ok", handleMergeOk)

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}

func (c *Context) Handle(typ string, handler func(maelstrom.Message, *Context) error) {
	c.Node.Handle(typ, func(m maelstrom.Message) error { return handler(m, c) })
}

func handleInit(m maelstrom.Message, context *Context) error {
	curNodeIdx, err := nodeId(context.Node.ID())
	if err != nil {
		return err
	}

	context.Counter = counter.Init(len(context.Node.NodeIDs()), curNodeIdx)
	go tick(context)
	return nil
}

func handleRead(m maelstrom.Message, context *Context) error {
	context.MsgId++
	body := map[string]any{
		"value":  context.Counter.Read(),
		"type":   "read_ok",
		"msg_id": context.MsgId,
	}

	context.Node.Reply(m, body)
	return nil
}

func handleAdd(m maelstrom.Message, context *Context) error {
	var body AddMessageBody
	if err := json.Unmarshal(m.Body, &body); err != nil {
		return err
	}

	context.Counter.Add(int(body.Delta))
	context.MsgId++
	context.Node.Reply(m, map[string]any{"type": "add_ok", "msg_id": context.MsgId})

	return nil
}

func handleMerge(m maelstrom.Message, context *Context) error {
	var body MergeMessageBody
	if err := json.Unmarshal(m.Body, &body); err != nil {
		return err
	}

	context.Counter.Merge(body.State)
	context.MsgId++
	context.Node.Send(m.Src, MergeMessageBody{
		Type:      "merge_ok",
		State:     context.Counter.State(),
		MsgId:     context.MsgId,
		InReplyTo: body.MsgId,
	})

	return nil
}

func handleMergeOk(m maelstrom.Message, context *Context) error {
	var body MergeMessageBody
	if err := json.Unmarshal(m.Body, &body); err != nil {
		return err
	}

	context.Counter.Merge(body.State)

	return nil
}

func tick(context *Context) {
	for {
		time.Sleep(300 * time.Millisecond)

		dst := selectNode(context.Node)
		context.Node.Send(dst, map[string]any{"type": "merge", "state": context.Counter.State()})
	}
}

func selectNode(node *maelstrom.Node) string {
	nodes := node.NodeIDs()
	self := node.ID()

	// dont send message to ourselves - rawndomize until another node is selected
	// do-while pattern
	var dst string
	for ok := true; ok; ok = dst == self {
		dst = nodes[rand.Intn(len(nodes))]
	}

	return dst
}

func nodeId(id string) (int, error) {
	return strconv.Atoi(id[1:])
}
