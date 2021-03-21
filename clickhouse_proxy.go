package clickhouse_proxy

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
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

	reloadedCh chan bool
	quitCh     chan bool
}

func RunProxy(clusterInfo *ClusterInfoType) (*ClickhouseProxy, error) {
	proxy := &ClickhouseProxy{
		clusterName: clusterInfo.name,
		nodesConn:   []*NodeType{},
		credentials: clusterInfo.credentials,

		nextNodeInd: 0,

		reloadedCh: make(chan bool),
		quitCh:     make(chan bool),
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

	timer := time.NewTimer(10 * time.Second)
	for {
		select {
		case <-timer.C:
			close(proxy.reloadedCh)
			return proxy, nil

		default:
			for _, node := range proxy.nodesConn {
				if node.IsHealthy() {
					close(proxy.reloadedCh)
					return proxy, nil
				}
			}
		}
	}
}

func (p *ClickhouseProxy) StopProxy() {
	for _, node := range p.nodesConn {
		close(node.quitCh)
	}
	close(p.quitCh)
}

func (p *ClickhouseProxy) ReloadConnections() error {
	p.reloadedCh = make(chan bool)
	defer close(p.reloadedCh)

	var errorNodes []string
	for _, node := range p.nodesConn {
		addr := "tcp://" + node.host
		if p.credentials != nil {
			addr += fmt.Sprintf("?username=%s&password=%s", p.credentials.username, p.credentials.password)
		}

		conn, err := sql.Open(driverName, addr)
		if err != nil {
			errorNodes = append(errorNodes, node.host)
		} else {
			node.CloseConn()

			node.conn = conn
			node.heartbeat = false
			node.quitCh = make(chan bool)

			go node.healthCheck()
		}
	}
	if len(errorNodes) > 0 {
		errorText := fmt.Sprintf("Errors with reset connection to %s", strings.Join(errorNodes, ", "))
		return errors.New(errorText)
	}

	return nil
}

func (p *ClickhouseProxy) ProxyExec(priorityNode, query string) (sql.Result, error) {
	select {
	case <-p.reloadedCh:
		node, err := p.getNextNode(priorityNode)
		if err != nil {
			return nil, err
		}

		return node.exec(query)
	}
}

func (p *ClickhouseProxy) ProxyQuery(priorityNode, query string) (*sql.Rows, error) {
	select {
	case <-p.reloadedCh:
		node, err := p.getNextNode(priorityNode)
		if err != nil {
			return nil, err
		}

		return node.query(query)
	}
}

func (p *ClickhouseProxy) ProxyBatchQuery(priorityNode, query string, batch [][]interface{}) error {
	select {
	case <-p.reloadedCh:
		node, err := p.getNextNode(priorityNode)
		if err != nil {
			return err
		}

		return node.batchQuery(query, batch)
	}
}

func (p *ClickhouseProxy) getNextNode(priorityNode string) (*NodeType, error) {
	nodeInd, roundRobin := p.getNodeIndAndRoundRobin(priorityNode)
	defer func() {
		if roundRobin {
			p.nextNodeInd = p.incNodeInd(p.nextNodeInd)
		}
	}()

	// 2 attempts to get current or next healthy node index
	nodeIsHealthy := true
	for attempt := 1; attempt <= 2; attempt++ {
		if !p.nodesConn[nodeInd].IsHealthy() {
			nodeIsHealthy = false
			for i := p.incNodeInd(nodeInd); i != nodeInd; i = p.incNodeInd(i) {
				if p.nodesConn[i].IsHealthy() {
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

	return p.nodesConn[nodeInd], nil
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
