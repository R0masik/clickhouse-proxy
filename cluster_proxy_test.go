package clickhouse_proxy

import (
	"testing"
)

func TestTest(t *testing.T) {
	nodes := []string{
		"10.50.3.227:9000",
		"10.50.3.128:9000",
		"10.50.3.129:9000",
	}
	clusterInfo := ClusterInfo("test", nodes)

	RunProxy(clusterInfo)
	_ = ProxyQuery("", "qwerty", nil)
	_ = ProxyQuery("", "qwerty", nil)
	_ = ProxyQuery("10.50.3.227:9000", "qwerty", nil)
	_ = ProxyQuery("", "qwerty", nil)
	_ = ProxyQuery("", "qwerty", nil)
	StopProxy()
}
