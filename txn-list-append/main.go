package main

import (
	"encoding/json"
	"log"
	"txn-list-append/state"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Context struct {
	Node  *maelstrom.Node
	State *state.State
}

type TxnMessageBody struct {
	Txn []state.Mop `json:"txn"`
}

func main() {
	node := maelstrom.NewNode()
	context := Context{node, state.NewState()}

	context.handle("txn", handleTxn)

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}

func (c *Context) handle(typ string, handler func(maelstrom.Message, *Context) error) {
	c.Node.Handle(typ, func(m maelstrom.Message) error { return handler(m, c) })
}

func handleTxn(m maelstrom.Message, c *Context) error {
	var body TxnMessageBody
	if err := json.Unmarshal(m.Body, &body); err != nil {
		return err
	}

	s := state.LoadState(c.Node)

	t2, s2 := state.Transact(body.Txn, s, c.Node)

	if err := state.SaveState(s, s2, c.Node); err != nil {
		return err
	}

	res := map[string]any{
		"type": "txn_ok",
		"txn":  t2,
	}

	c.Node.Reply(m, res)
	return nil
}
