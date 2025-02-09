# Zattera

![Build](https://github.com/rascio/zattera/actions/workflows/build-and-verify.yml/badge.svg)

> I'm too lazy to find a better name, so it is just a translation

## What is it?
A "bad" implementation of a raft consensus algorithm, just for fun.

## Testing
`scripts/` folder contains scripts to start nodes/cluster and clients:

- `start-node.sh`: Starts a node with the given parameters, support a simple counter and a KV store
- `key-value.sh`: CLI to interact with the KV store

both support `-h` to show the help message.

### KV store example usage
#### Start cluster nodes
```shell
./scripts/start-node.sh -id 1 -cluster-size 3 -kv
```
`-id` can be any number, but it must be unique in the cluster.
`-cluster-size` make the node aware of the first N nodes in the cluster.

#### Start the KV store CLI
```shell
./scripts/key-value.sh shell
```


### Interact with Server

#### Read the log
```shell
curl http://localhost:8081/entries
```

#### Register a new node to the cluster
This needs to be done before starting it
```shell
curl -v http://localhost:8081/raft/request -d '{"type":"io.r.raft.protocol.LogEntry.ConfigurationChange","new":[{"id":"N4","host":"localhost","port":8084}]}' -H 'Content-Type: application/json'
```
and then
```shell
./scripts/start-node.sh -id 4 -cluster-size 3 -kv
```

### Debug

#### RaftMachine Logs
```shell
# Get logs for a single node
cat .logs/test-logs.out| grep -E "NodeId=T1" | pbcopy

```
#### RaftMachineTest
```shell
# Get Raft log dumps
cat .logs/test-logs.out | ggrep -oPz "(?s)Dump_log.*?End_Dump\s" | pbcopy
# Entries applied to state machine (replace T1 with actual node id)
cat .logs/test-logs.out| grep -E "NodeId=T1 \| _event=\"(Applied|Set-A)" | pbcopy
```