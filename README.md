# Zattera

> I'm too lazy to find a better name, so it is just a translation

## What is it?
A bad implementation of a raft consensus algorithm, just for fun.

## Commands

Start with a slow heartbeat and debug enabled
```shell
mvn -q exec:java -Dexec.mainClass=io.r.raft.MainKt \
-Dexec.args="N1 --port 8081 --peer N2=http://localhost:8082 --peer N3=http://localhost:8083 --election-timeout=5000 --heartbeat-timeout=1000 --election-jitter=500 --debug-messages"
```

Start with a normal heartbeat
```shell
mvn -q exec:java -Dexec.mainClass=io.r.raft.MainKt \
-Dexec.args="N1 --port 8081 --peer N2=http://localhost:8082 --peer N3=http://localhost:8083 --election-timeout=500 --heartbeat-timeout=50 --election-jitter=100"
```

Start a node and kill the entire cluster if it fails
```shell
mvn -q exec:java -Dexec.mainClass=io.r.raft.MainKt \
  -Dexec.args="N1 --port 8081 --peer N2=http://localhost:8082 --peer N3=http://localhost:8083 --election-timeout=2000 --heartbeat-timeout=500 --election-jitter=300" \
  || kill -1 $(jps -v | grep zattera | cut -d ' ' -f1)
```