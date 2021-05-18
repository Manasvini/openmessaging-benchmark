#!/bin/bash

directory=$1
driver=$2

KAFKA_PATH=~/kafka-2.4.0-src
PULSAR_PATH=~/apache-pulsar-2.5.0

restart_kafka()
{
    #stop kafka
    $KAFKA_PATH/bin/kafka-server-stop.sh
    
    #delete all topics
    $KAFKA_PATH/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic 'test-topic.*'

    #remove logs
    rm -rf /tmp/kafka-logs

    #start kafka
    $KAFKA_PATH/bin/kafka-server-start.sh $KAFKA_PATH/config/server.properties & > /dev/null 
    #wait for broker to start up
    sleep 30
}

shutdown_kafka()
{
   $KAFKA_PATH/bin/kafka-server-stop.sh
}

restart_pulsar()
{
    #stop pulsar
    $PULSAR_PATH/bin/pulsar-daemon stop standalone
    $PULSAR_PATH/bin/pulsar-daemon start standalone -a 127.0.0.1 -nss
    sleep 30
}

shutdown_pulsar()
{
    $PULSAR_PATH/bin/pulsar-daemon stop standalone
}

shutdown_all()
{
    shutdown_kafka
    shutdown_pulsar
}

run_benchmark()
{
    #arg1: driver config e.g, driver-kafka/kafka.yaml
    #arg2: moving/static
    #arg3: workload file
    echo $1 $2 $3
    bin/benchmark --drivers $1 -t $2 $3
}

for entry in "$directory"/*.yaml
do
     echo "$entry"
     if [ $driver = "kafka" ]
     then
        restart_kafka
        run_benchmark "driver-kafka/kafka.yaml" "moving" $entry
     
     elif [ $driver = "pulsar" ]
     then
        restart_pulsar
        run_benchmark "driver-pulsar/pulsar.yaml" "moving" $entry
     fi
done

shutdown_all
