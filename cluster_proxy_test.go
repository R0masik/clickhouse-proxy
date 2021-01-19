package clickhouse_proxy

import (
	"fmt"
	"testing"
)

func TestTest(t *testing.T) {
	nodes := []string{
		"10.50.3.128:9100",
		"10.50.3.127:9000",
		"10.50.3.129:9100",
	}
	clusterInfo := ClusterInfo("test", nodes)

	proxy, err := RunProxy(clusterInfo)
	if err != nil {
		fmt.Println(err)
		return
	}

	// 1
	rows, err := proxy.ProxyQuery("10.50.3.127:9000", "show databases", nil)
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

	// 2
	rows, err = proxy.ProxyQuery("", "show databases", nil)
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

	// 3
	rows, err = proxy.ProxyQuery("", "show databases", nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	for rows.Next() {
		var a string
		err := rows.Scan(&a)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(a)
		}
	}

	proxy.StopProxy()
}
