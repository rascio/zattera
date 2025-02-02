#!/bin/bash

program_arguments="$@"

# if --peer is not provided, add default value
if [[ $program_arguments != *"--peer"* ]]; then
  program_arguments="$program_arguments --peer N1=localhost:8081"
fi
mvn -q exec:java -Dexec.mainClass=io.r.kv.KeyValueCliKt -Dexec.args="$program_arguments"
