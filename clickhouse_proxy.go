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

var proxy *clickhouseProxy

func RunProxy(clusterInfo *ClusterInfoType) error {
	proxy = &clickhouseProxy{
		clusterName: clusterInfo.name,
		nodesConn:   []*NodeType{},
		credentials: clusterInfo.credentials,

		nextNodeInd: 0,

		quitCh: make(chan bool),
	}

	for _, node := range clusterInfo.nodes {
		addr := "tcp://" + node
		if proxy.credentials != nil {
			addr += fmt.Sprintf("?username=%s&password=%s", proxy.credentials.username, proxy.credentials.password)
		}
		conn, err := sql.Open(driverName, addr)
		if err != nil {
			close(proxy.quitCh)
			return err
		}

		nodeConn := &NodeType{
			conn: conn,
			host: node,

			heartbeat: false,
		}
		nodeConn.updateHeartbeat()
		go nodeConn.healthCheck()

		proxy.nodesConn = append(proxy.nodesConn, nodeConn)
	}

	return nil
}

func StopProxy() {
	close(proxy.quitCh)
}

func ProxyQuery(priorityNode string, query string, batch [][]interface{}) (*sql.Rows, error) {
	nodeInd, roundRobin := proxy.getNodeIndAndRoundRobin(priorityNode)
	defer func() {
		if roundRobin {
			proxy.nextNodeInd = proxy.incNodeInd(proxy.nextNodeInd)
		}
	}()

	// get current or next healthy node index
	if !proxy.nodesConn[nodeInd].heartbeat {
		for i := proxy.incNodeInd(nodeInd); i == nodeInd; i = proxy.incNodeInd(i) {
			if proxy.nodesConn[i].heartbeat {
				nodeInd = i
				break
			}
		}
	}

	node := proxy.nodesConn[nodeInd]
	if batch == nil {
		return node.execQuery(query)
	} else {
		return nil, node.execBatchQuery(query, batch)
	}
}

func (p *clickhouseProxy) getNodeIndAndRoundRobin(priorityNode string) (nodeInd int, roundRobin bool) {
	if priorityNode != "" {
		for i, node := range p.nodesConn {
			if priorityNode == node.host {
				return i, false
			}
		}

		fmt.Println("Priority node is not found in cluster, round-robin will be used")
		return p.nextNodeInd, true

	} else {
		return p.nextNodeInd, true
	}
}

func (p *clickhouseProxy) incNodeInd(i int) int {
	if i < len(p.nodesConn)-1 {
		return i + 1
	} else {
		return 0
	}
}
