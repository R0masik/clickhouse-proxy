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
}

// goroutine
func (n *NodeType) healthCheck() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
	}()

	for {
		select {
		case <-ticker.C:
			err := n.conn.Ping()
			if err == nil {
				n.heartbeat = true
			} else {
				n.heartbeat = false
			}

		case <-proxy.quitCh:
			return
		}
	}
}

// goroutine
func (n *NodeType) tryToReconnect() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
	}()

	for {
		select {
		case <-ticker.C:
			addr := "tcp://" + n.host
			if proxy.credentials != nil {
				addr += fmt.Sprintf("?username=%s&password=%s", proxy.credentials.username, proxy.credentials.password)
			}
			conn, err := sql.Open(driverName, addr)
			if err != nil {
				fmt.Println("Proxy has connected to host " + n.host)
				n.conn = conn
				n.heartbeat = true
				go n.healthCheck()

				return
			}

		case <-proxy.quitCh:
			return
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
