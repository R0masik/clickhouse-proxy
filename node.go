package clickhouse_proxy

import (
	"database/sql"
	"fmt"
	"time"
)

type NodeType struct {
	conn        *sql.DB
	host        string
	credentials *CredentialsType

	heartbeat bool

	quitCh chan bool
}

func newNode(host string, credentials *CredentialsType) *NodeType {
	return &NodeType{
		conn:        nil,
		host:        host,
		credentials: credentials,

		heartbeat: false,

		quitCh: make(chan bool),
	}
}

func (n *NodeType) connect() error {
	addr := "tcp://" + n.host
	if n.credentials != nil {
		addr += fmt.Sprintf("?username=%s&password=%s", n.credentials.username, n.credentials.password)
	}
	conn, err := sql.Open(driverName, addr)
	if err != nil {
		return err
	}
	n.conn = conn

	return nil
}

// goroutine
func (n *NodeType) healthCheck() {
	// first try
	n.updateHeartbeat()

	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
	}()

	for {
		select {
		case <-ticker.C:
			n.updateHeartbeat()

		case <-n.quitCh:
			return
		}
	}
}

func (n *NodeType) updateHeartbeat() {
	connLost := false
	if n.conn != nil {
		if err := n.conn.Ping(); err != nil {
			connLost = true
		}
	} else {
		connLost = true
	}

	if connLost {
		n.heartbeat = false
		err := n.connect()
		if err == nil {
			n.heartbeat = true
		}
	} else {
		n.heartbeat = true
	}
}

func (n *NodeType) exec(query string) (sql.Result, error) {
	return n.conn.Exec(query)
}

func (n *NodeType) query(query string) (*sql.Rows, error) {
	return n.conn.Query(query)
}

func (n *NodeType) batchQuery(query string, batch [][]interface{}) error {
	tx, err := n.conn.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, batchItem := range batch {
		_, err := stmt.Exec(batchItem...)
		if err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (n *NodeType) IsHealthy() bool {
	return n.heartbeat
}

func (n *NodeType) CloseConn() error {
	close(n.quitCh)
	return n.conn.Close()
}
