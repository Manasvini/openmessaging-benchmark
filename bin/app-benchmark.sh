#!/bin/bash

if [ -d "./lib" ]; then
        CLASSPATH=$CLASSPATH:lib/*
else
    CLASSPATH=benchmark-framework/target/classes:`cat benchmark-framework/target/classpath.txt`
fi

JVM_MEM="-Xms4G -Xmx4G -XX:+UseG1GC"
JVM_GC_LOG=" -XX:+PrintGCDetails -XX:+PrintGCApplicationStoppedTime  -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=5 -XX:GCLogFileSize=64m  -Xloggc:/dev/shm/benchmark-client-gc_%p.log"

echo $*

java -server -cp $CLASSPATH $JVM_MEM io.openmessaging.benchmark.applicationworkerbenchmark.ApplicationWorkerBenchmark $*
