package clickhouse_proxy

import (
	"database/sql"
	"time"
)

type NodeType struct {
	conn *sql.DB
	host string

	heartbeat bool

	quitCh chan bool
}

func (n *NodeType) IsHealthy() bool {
	return n.heartbeat
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
	err := n.conn.Ping()
	if err == nil {
		if n.heartbeat == false {
			n.heartbeat = true
		}
	} else {
		if n.heartbeat == true {
			n.heartbeat = false
		}
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

	// there may be memory leaks due to its absence
	defer func() {
		stmt = nil
		tx = nil
	}()

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
