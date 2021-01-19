package clickhouse_proxy

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
)

const (
	driverName = "clickhouse"
	pingPeriod = 5 * time.Second
)

type ClickhouseProxy struct {
	clusterName string
	nodesConn   []*NodeType
	credentials *CredentialsType

	nextNodeInd int

	quitCh chan bool
}

func RunProxy(clusterInfo *ClusterInfoType) (*ClickhouseProxy, error) {
	proxy := &ClickhouseProxy{
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
			return nil, err
		}

		nodeConn := &NodeType{
			conn: conn,
			host: node,

			heartbeat: false,

			quitCh: make(chan bool),
		}
		go nodeConn.healthCheck()

		proxy.nodesConn = append(proxy.nodesConn, nodeConn)
	}

	return proxy, nil
}

func (p *ClickhouseProxy) StopProxy() {
	for _, node := range p.nodesConn {
		close(node.quitCh)
	}
	close(p.quitCh)
}

func (p *ClickhouseProxy) ProxyQuery(priorityNode string, query string, batch [][]interface{}) (*sql.Rows, error) {
	nodeInd, roundRobin := p.getNodeIndAndRoundRobin(priorityNode)
	defer func() {
		if roundRobin {
			p.nextNodeInd = p.incNodeInd(p.nextNodeInd)
		}
	}()

	// get current or next healthy node index
	// 2 attempts
	nodeIsHealthy := true
	for attempt := 1; attempt <= 2; attempt++ {
		if !p.nodesConn[nodeInd].heartbeat {
			nodeIsHealthy = false
			for i := p.incNodeInd(nodeInd); i != nodeInd; i = p.incNodeInd(i) {
				if p.nodesConn[i].heartbeat {
					nodeInd = i
					nodeIsHealthy = true
					break
				}
			}
		}

		if nodeIsHealthy {
			break
		}
		if attempt == 1 {
			time.Sleep(2 * time.Second)
		}
	}
	if !nodeIsHealthy {
		return nil, errors.New("there are no healthy nodes")
	}

	node := p.nodesConn[nodeInd]
	if batch == nil {
		return node.execQuery(query)
	} else {
		return nil, node.execBatchQuery(query, batch)
	}
}

func (p *ClickhouseProxy) getNodeIndAndRoundRobin(priorityNode string) (nodeInd int, roundRobin bool) {
	if priorityNode != "" {
		for i, node := range p.nodesConn {
			if priorityNode == node.host {
				return i, false
			}
		}

		return p.nextNodeInd, true

	} else {
		return p.nextNodeInd, true
	}
}

func (p *ClickhouseProxy) incNodeInd(i int) int {
	if i < len(p.nodesConn)-1 {
		return i + 1
	} else {
		return 0
	}
}
