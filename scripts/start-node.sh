#!/bin/bash

help_message="
Usage: start_node.sh -id <node_id> -cluster-size <cluster_size> [-debug] [-store-log]
  -id: node number as an argument
    use it to pick the id and set the port, eg. N1 -> 8081, N2 -> 8082, etc.
  -cluster-size: core cluster size
    use it to set the peers, eg. N1, N2, N3, N4, etc.
  -debug: enable debug messages
    use it to enable debug messages and use slower timeouts, eg. --election-timeout=5000 --heartbeat-timeout=1000 --election-jitter=500
  -store-log: enable log persistence
    store logs in the .logs directory
  -kv: enable key-value store
    enable key-value store
  -help: show this help message
"
while [[ $# -gt 0 ]]
do
  key="$1"
  case $key in
    -id)
      node="$2"
      shift
      shift
      ;;
    -cluster-size)
      cluster_size="$2"
      shift
      shift
      ;;
    -debug)
      debug=true
      shift
      ;;
    -store-log)
      store_log=true
      shift
      ;;
    -kv)
      kv=true
      shift
      ;;
    -help)
      echo "$help_message"
      exit 0
      ;;
    *)
      shift
      ;;
  esac
done

port=$((8080 + $node))
# define timeout_params parameters
if [ "$debug" = true ]; then
  timeout_params="--election-timeout=5000 --heartbeat-timeout=1000 --election-jitter=500 --debug-messages"
else
  timeout_params="--election-timeout=300 --heartbeat-timeout=100 --election-jitter=100"
fi


# iterate 'n' times and build the --peer argument where 'n' is the value of the second argument
for i in $(seq 1 $cluster_size); do
    peers="$peers --peer N$i=localhost:$((8080 + $i))"
done

program_args="N$node --port $port $peers $timeout_params"

#if key-value store is enabled, add the --state-machine argument
if [ "$kv" = true ]; then
  program_args="$program_args --state-machine io.r.kv.StringsKeyValueStore"
fi


echo "Starting node $node on port $port"

#launch the maven command with tee if store_log is enabled
if [ "$store_log" = true ]; then
  mvn -q process-resources exec:java \
  -Dexec.mainClass=io.r.raft.MainKt -Dexec.args="$program_args" | tee ".logs/N$node.log"
else
  mvn -q -Dstyle.color=always process-resources exec:java \
  -Dexec.mainClass=io.r.raft.MainKt -Dexec.args="$program_args"
fi