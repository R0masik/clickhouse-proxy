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

func ProxyQuery(priorityNode string, query string, batch [][]interface{}) (*sql.Rows, error) {
	nodeInd, roundRobin := proxy.getNodeIndAndRoundRobin(priorityNode)
	defer func() {
		if roundRobin {
			proxy.incNextNodeInd()
		}
	}()

	if batch == nil {
		return proxy.nodesConn[nodeInd].execQuery(query)
	} else {
		return nil, proxy.nodesConn[nodeInd].execBatchQuery(query, batch)
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

func (p *clickhouseProxy) incNextNodeInd() {
	if p.nextNodeInd == len(p.nodesConn)-1 {
		p.nextNodeInd = 0
	} else {
		p.nextNodeInd++
	}
}
