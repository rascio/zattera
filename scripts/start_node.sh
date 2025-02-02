#!/bin/bash

#-id parameter: node number as an argument
#   use it to set the port, eg. N1 -> 8081, N2 -> 8082, etc.
#-cluster-size parameter: core cluster size
#   use it to set the peers, eg. N1 -> N2, N3, N4, etc.
#-debug parameter: enable debug messages
#   use it to enable debug messages, eg. --debug-messages and use slower timeouts, eg. --election-timeout=5000 --heartbeat-timeout=1000 --election-jitter=500

#read input parameters shifting arguments
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
    -help)
      echo "Usage: start_node.sh -id <node_id> -cluster-size <cluster_size> [-debug]"
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

echo "Starting node $node on port $port"

mvn -q -Dstyle.color=always process-resources exec:java -Dexec.mainClass=io.r.raft.MainKt \
  -Dexec.args="N$node --port $port $peers $timeout_params" \
  | tee >(sed 's/\x1b\[[0-9;]*m//g' > .logs/N$node.log)
