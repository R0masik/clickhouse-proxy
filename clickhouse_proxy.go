package clickhouse_proxy

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
)

const (
	driverName = "clickhouse"
	pingPeriod = 5 * time.Second
)

type clickhouseProxy struct {
	clusterName string
	nodesConn   []*NodeType
	credentials *CredentialsType

	nextNodeInd int

	quitCh chan bool
}

type NodeType struct {
	conn *sql.DB
	host string

	heartbeat bool
}

var proxy *clickhouseProxy

func RunProxy(clusterInfo *ClusterInfoType) {
	proxy = &clickhouseProxy{
		clusterName: clusterInfo.name,
		nodesConn:   []*NodeType{},
		credentials: clusterInfo.credentials,

		nextNodeInd: 0,

		quitCh: make(chan bool),
	}

	for _, node := range clusterInfo.nodes {
		nodeConn := &NodeType{
			conn: nil,
			host: node,

			heartbeat: false,
		}

		addr := "tcp://" + node
		if proxy.credentials != nil {
			addr += fmt.Sprintf("?username=%s&password=%s", proxy.credentials.username, proxy.credentials.password)
		}
		conn, err := sql.Open(driverName, addr)
		if err != nil {
			fmt.Println(err)
			go nodeConn.tryToReconnect()
		} else {
			fmt.Println("Proxy has connected to host " + node)
			nodeConn.conn = conn
			nodeConn.heartbeat = true
			go nodeConn.healthCheck()
		}

		proxy.nodesConn = append(proxy.nodesConn, nodeConn)
	}
}

func StopProxy() {
	close(proxy.quitCh)
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
