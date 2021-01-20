package clickhouse_proxy

import (
	"fmt"
	"os"
	"testing"
)

var proxy *ClickhouseProxy

func TestMain(m *testing.M) {
	nodes := []string{
		"10.50.3.128:9100",
		"10.50.3.127:9000",
		"10.50.3.129:9100",
	}
	clusterInfo := ClusterInfo("test", nodes)

	var err error
	proxy, err = RunProxy(clusterInfo)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	exitCode := m.Run()

	proxy.StopProxy()

	os.Exit(exitCode)
}

func TestProxyExec(t *testing.T) {
	createDbQuery := "create database if not exists db_test"
	_, err := proxy.ProxyExec("", createDbQuery)
	if err != nil {
		t.Fatal(err)
		return
	}

	createTableQuery := "create table if not exists db_test.table_test(a Int32) ENGINE = MergeTree() ORDER BY a"
	_, err = proxy.ProxyExec("", createTableQuery)
	if err != nil {
		t.Fatal(err)
		return
	}
}

func TestProxyBatchQuery(t *testing.T) {
	insertQuery := "insert into db_test.table_test(a) values (?)"
	batch := [][]interface{}{
		{1},
	}
	err := proxy.ProxyBatchQuery("", insertQuery, batch)
	if err != nil {
		t.Fatal(err)
		return
	}
}

func TestProxyQuery(t *testing.T) {
	selectQuery := "select  * from db_test.table_test"
	rows, err := proxy.ProxyQuery("", selectQuery)
	if err != nil {
		t.Fatal(err)
		return
	}
	for rows.Next() {
		var a string
		err := rows.Scan(&a)
		if err != nil {
			t.Fatal(err)
		} else {
			fmt.Println(a)
		}
	}
}
