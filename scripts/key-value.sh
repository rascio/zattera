#!/bin/bash

program_arguments="$@"

# if -p or -h are missing add them
if [[ $program_arguments != *"-p"* ]]; then
  program_arguments="$program_arguments -p 8081"
fi
if [[ $program_arguments != *"-h"* ]]; then
  program_arguments="$program_arguments -h localhost"
fi
mvn -q exec:java -Dexec.mainClass=io.r.kv.KeyValueCliKt -Dexec.args="$program_arguments"
