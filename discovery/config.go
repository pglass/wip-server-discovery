package discovery

type Config struct {
	// Addresses is a DNS name or exec command for go-netaddrs.
	Addresses string
	// GRPCPort is the gRPC port to connect to. This must be the
	// same for all Consul servers for now. Defaults to 8502.
	GRPCPort int

	// ServerWatchDisabled disables opening the ServerWatch gRPC
	// stream. This should be used when your Consul servers are
	// behind a load balancer, for example, since the server addresses
	// returned in the ServerWatch stream will differ from the load
	// balancer address.
	ServerWatchDisabled bool

	TLS         TLSConfig
	Credentials Credentials
}

type TLSConfig struct {
	// CertFile is the optional client certificate for TLS
	// conections to servers.
	CertFile string
	// KeyFile is the optional private key for TLS connections to
	// servers.
	KeyFile string
	// CACertsPath is the optional file or directory path of
	// PEM-encoded CA certificat(es) to trust. Required if the
	// server certificate is otherwise not trusted.
	CACertsPath string
	// ServerName is optional the name of the server, used as the
	// SNI value and for verify the server TLS certificate.
	ServerName string
	// InsecureSkipVerify disables TLS server certificate
	// verification.
	InsecureSkipVerify bool
}

type Credentials struct {
	// Type is either "static" for a statically-configured ACL
	// token, or "login" to obtain an ACL token by logging into a
	// Consul auth method.
	Type string

	// Static is used if Type is "static".
	Static StaticTokenCredential

	// Login is used if Type is "login".
	Login LoginCredential
}

type StaticTokenCredential struct {
	// Token is a static ACL token used for gRPC requests to the
	// Consul servers.
	Token string
}

type LoginCredential struct {
	// Method is the name of the Consul auth method.
	Method string
	// Namespace is the namespace containing the auth method.
	Namespace string
	// Partition is the partition containing the auth method.
	Partition string
	// Datacenter is the datacenter containing the auth method.
	Datacenter string
	// Bearer is the bearer token presented to the auth method.
	Bearer string
	// BearerPath is the path to a file containing the bearer token.
	BerearPath string
	// Meta is the abitrary set of key-value pairs to attach to the
	// token. These are included on the Description field.
	Meta map[string]string
}
