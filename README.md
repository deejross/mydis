# Mydis
Distributed key/value library, server, and client. Inspired by Redis, Mydis is written entirely in Go and can be used as a library, embedded into an existing application, or as a standalone client/server.

## Basics
Mydis supports many of the [Redis commands](https://redis.io/commands) and uses [Raft](github.com/hashicorp/raft) to maintain consistency between instances. This means that clusters should have an odd number of instances. It exposes both a REST API and a Redis-compatible interface.

## Read/Write Consistency
By default, Mydis uses an eventual consistency model, meaning that write operations block the requestor until the leader instance has committed the write. The leader then replicates that operation to all followers in the cluster. Immediately reading back a value on a follower that was just written could return the old (stale) value, as it may not have been replicated to all followers yet. This allows for better write performance, but may not be acceptable in some cases.

Mydis offers both read and write consistency mode options on a per-command basis: eventual (default), and strong. Depending on type of operation being performed, this is how the different modes behave:

* Read operations
    * Eventual: The operation is processed on the instance that recieved the request
    * Strong: The operation is forwarded to the leader instance for processing, which ensures the latest value is returned, but it slower
* Write operations
    * Eventual: The operation is blocked until the leader instance has committed the write
    * Strong: The operation is blocked until all instances have committed the write

The cluster can also be configured to enforce strong write consistency for all write operations at the cost of write performance.

## Ports
Mydis uses a number of ports in order to function properly. The ports are configurable, but the defaults are listed below.

### Client
These are the ports that should be exposed to clients:
* 2380 - HTTP(s) REST API
* 2381 - Redis protocol

Client ports can be load balanced across all instances in the cluster, allowing for read-scalability. Write commands issued to follower instances are automatically forwarded over the RPC port to the leader instance for processing.

### Operational
These ports are required for proper cluster operation and should not be exposed to clients. Instances need to communicate with each other on these ports:
* 2382 - Raft port for maintaining cluster consistency
* 2383 - RPC port used for issuing commands on other instances in the cluster

**NOTE**: While the Raft port is configurable, the RPC port will always be the Raft port number, plus one. This is required for instances to be able to discover what port to use for RPC communications.

# TODO
* KV API for higher-level Redis-like operations
* HTTP API
* Redis API (https://github.com/tidwall/redcon)
* Authentication with tokens that can be limited to certain key prefixes (ACLs)
* Raft/RPC symetric encryption