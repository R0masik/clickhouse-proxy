package clickhouse_proxy

type ClusterInfoType struct {
	name        string
	nodes       []string
	credentials *CredentialsType
}

type CredentialsType struct {
	username string
	password string
}

func ClusterInfo(name string, nodes []string) *ClusterInfoType {
	return &ClusterInfoType{
		name:        name,
		nodes:       nodes,
		credentials: nil,
	}
}

func (info *ClusterInfoType) ApplyCredentials(username, password string) *ClusterInfoType {
	info.credentials = &CredentialsType{
		username: username,
		password: password,
	}
	return info
}
