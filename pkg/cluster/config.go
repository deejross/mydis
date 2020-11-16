package cluster

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/go-sockaddr"
	"github.com/hashicorp/go-sockaddr/template"
)

const (
	// WriteConsistencyEventual represents the `eventual` value for Config.WriteConsistency.
	WriteConsistencyEventual = "eventual"

	// WriteConsistencyStrong represents the `strong` value for Config.WriteConsistency.
	WriteConsistencyStrong = "strong"
)

// Config for Cluster object.
type Config struct {
	// DataDir is the directory to use for data storage.
	// Defaults to `data`.
	DataDir string

	// BindAddress is the IP to bind to for inter-cluster communications.
	// This takes priority over other `Bind*` options.
	// Defaults to `127.0.0.1` if no other `Bind*` options are used.
	BindAddress string

	// BindInterface will use the given network interface's IP address for binding.
	// The following is the equivalent BindTemplate string:
	//	{{GetInterfaceIP "en0"}}
	BindInterface string

	// BindPrivateIP will use the first routable IP address discovered.
	// The following is the equivalent BindTemplate string:
	//	{{GetPrivateInterfaces | attr "address"}}
	BindPrivateIP bool

	// BindPublicIP will use the first routable public IP address discovered.
	// The following is the equivalent BindTemplate string:
	//	{{GetPublicInterfaces | attr "address"}}
	BindPublicIP bool

	// BindTemplate takes a template string as defined here:
	// https://pkg.go.dev/github.com/hashicorp/go-sockaddr/template
	BindTemplate string

	// HTTPPort is the port for clients to use for the HTTP interface.
	// Defaults to `2380`. Set to `-1` to disable.
	HTTPPort string

	// RedisPort is the port for clients to use for the Redis interface.
	// Defaults to `2381`. Set to `-1` to disable.
	RedisPort string

	// RaftPort is the port to use for Raft communications between instances.
	// Defaults to `2382`.
	RaftPort string

	// RPCPort is the port to use for RPC communications between instances.
	// This will always be set to RaftPort+1 (i.e. `2383` unless RaftPort is changed).
	RPCPort string

	// Join is a comma-separated list of at least one IPv4, IPv6, or DNS address
	// of other known cluster nodes. If a DNS address is used that has multiple A
	// records, all A records are used except any that match `BindAddress`. This is
	// useful in Kubernetes environments where the cluster's pods are exposed using
	// a headless service (i.e. `.spec.clusterIP: None`) and specifying the service
	// URL for `Join` (i.e.: `my-cluster.default.svc`).
	Join string

	// Bootstrap when `true` initializes a new cluster faster but can only be `true`
	// for the first instance of a new cluster.
	Bootstrap bool

	// Name of the cluster. Defaults to `mydis`.
	Name string

	// InstanceID for this instance. Defaults to `{{ BindAddress }}:{{ BindPort }}`.
	InstanceID string

	// RaftTimeout is how long to wait for writes to the cluster to complete before timing out.
	// Defaults to 10 seconds.
	RaftTimeout time.Duration

	// DefaultWriteConsistency can be either set to `eventual` (default) or `strong`.
	// When `eventual` is used, writes will block until the Leader has committed them to
	// storage. Clients have both a ReadConsistency and WriteConsistency option that can be used
	// to prevent stale reads for critical queries.
	// When `strong` is used, writes will block until all instances in the cluster have
	// committed them to storage. This prevents stale reads on Followers at the cost
	// of write performance.
	DefaultWriteConsistency string

	// join is a resolved list of IP addresses to attempt a cluster join operation on.
	joinAddrs []string
}

// Validate the Config and set sane defaults.
func (c *Config) Validate() error {
	if len(c.DataDir) == 0 {
		c.DataDir = "data"
	}

	if len(c.HTTPPort) == 0 {
		c.HTTPPort = "2380"
	}

	if len(c.RedisPort) == 0 {
		c.RedisPort = "2381"
	}

	if len(c.RaftPort) == 0 {
		c.RaftPort = "2382"
	}

	// always make the RPC port RaftPort + 1 for easy discovery
	raftPort, _ := strconv.Atoi(c.RaftPort)
	c.RPCPort = strconv.Itoa(raftPort + 1)

	// determine BindAddress value from the various Bind options
	if len(c.BindAddress) == 0 {
		if len(c.BindInterface) == 0 {
			ip, err := sockaddr.GetInterfaceIP(c.BindInterface)
			if err != nil {
				return fmt.Errorf("unable to determine IP address for interface: %s: %v", c.BindInterface, err)
			}
			c.BindAddress = ip
		}

		if c.BindPrivateIP && len(c.BindAddress) == 0 {
			ip, err := sockaddr.GetPrivateIP()
			if err != nil {
				return fmt.Errorf("unable to determine private IP: %v", err)
			}
			c.BindAddress = ip
		}

		if c.BindPublicIP && len(c.BindAddress) == 0 {
			ip, err := sockaddr.GetPublicIP()
			if err != nil {
				return fmt.Errorf("unable to determine public IP: %v", err)
			}
			c.BindAddress = ip
		}

		if len(c.BindTemplate) > 0 && len(c.BindAddress) == 0 {
			ip, err := template.Parse(c.BindTemplate)
			if err != nil {
				return fmt.Errorf("template parse failed: %v", err)
			}
			c.BindAddress = ip
		}

		if len(c.BindAddress) == 0 {
			c.BindAddress = "127.0.0.1"
		}
	}

	// determine the list of IPs to perform a cluster join against
	c.joinAddrs = []string{}
	if len(c.Join) > 0 {
		addrs := strings.Split(c.Join, ",")
		for _, addr := range addrs {
			addr = strings.TrimSpace(addr)
			if err := c.resolveAddress(addr); err != nil {
				return err
			}
		}
	}

	if len(c.Name) == 0 {
		c.Name = "mydis"
	}

	if len(c.InstanceID) == 0 {
		c.InstanceID = net.JoinHostPort(c.BindAddress, c.RaftPort)
	}

	if c.RaftTimeout == 0 {
		c.RaftTimeout = 10 * time.Second
	}

	if len(c.DefaultWriteConsistency) == 0 {
		c.DefaultWriteConsistency = WriteConsistencyEventual
	} else {
		switch c.DefaultWriteConsistency {
		case WriteConsistencyEventual, WriteConsistencyStrong:
		default:
			return fmt.Errorf("unknown WriteConsistency value: %s", c.DefaultWriteConsistency)
		}
	}

	return nil
}

// resolveAddress attempts to resolve the given address to one or more IP addresses
// which then get added to `joinAddrs`.
func (c *Config) resolveAddress(addr string) error {
	addr, port, _ := net.SplitHostPort(addr)

	ip := net.ParseIP(addr)
	if ip != nil {
		c.joinAddrs = append(c.joinAddrs, net.JoinHostPort(addr, port))
		return nil
	}

	addrs, err := net.LookupHost(addr)
	if err != nil {
		return fmt.Errorf("unable to perform DNS lookup on: %s :%v", addr, err)
	}

	for _, addr := range addrs {
		ip := net.ParseIP(addr)
		if ip != nil {
			c.joinAddrs = append(c.joinAddrs, net.JoinHostPort(addr, port))
		}
	}

	return nil
}
