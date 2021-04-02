package clickhouse_proxy

type ClusterInfoType struct {
	name        string
	hosts       []string
	credentials *CredentialsType
}

type CredentialsType struct {
	username string
	password string
}

func ClusterInfo(name string, hosts []string) *ClusterInfoType {
	return &ClusterInfoType{
		name:        name,
		hosts:       hosts,
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
