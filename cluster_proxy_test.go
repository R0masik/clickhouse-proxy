package clickhouse_proxy

import (
	"fmt"
	"testing"
)

func TestTest(t *testing.T) {
	nodes := []string{
		"10.50.3.227:9100",
		"10.50.3.128:9000",
		"10.50.3.129:9000",
	}
	clusterInfo := ClusterInfo("test", nodes)

	err := RunProxy(clusterInfo)
	if err != nil {
		fmt.Println(err)
		return
	}

	rows, err := ProxyQuery("", "show databases", nil)
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

	rows, err = ProxyQuery("", "show databases", nil)
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

	//_, _ = ProxyQuery("10.50.3.227:9000", "qwerty", nil)
	//_, _ = ProxyQuery("", "qwerty", nil)
	//_, _ = ProxyQuery("", "qwerty", nil)
	StopProxy()
}
