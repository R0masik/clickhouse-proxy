package clickhouse_proxy

import (
	"database/sql"
	"fmt"
	"time"
)

type NodeType struct {
	conn *sql.DB
	host string

	heartbeat bool

	quitCh chan bool
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
			fmt.Println("Connected to "+n.host)
			n.heartbeat = true
		}
	} else {
		if n.heartbeat == true {
			fmt.Println("Disconnected from "+n.host)
			n.heartbeat = false
		}
	}
}

func (n *NodeType) execQuery(query string) (*sql.Rows, error) {
	fmt.Println("execQuery " + n.host)

	return n.conn.Query(query)
}

func (n *NodeType) execBatchQuery(query string, batch [][]interface{}) error {
	fmt.Println("execBatchQuery " + n.host)

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
