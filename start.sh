#!/bin/bash

set -ex

source env.sh
java $JAVA_OPTS -jar target/eventbus-registration-bench.jar -cluster
