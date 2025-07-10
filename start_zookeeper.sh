#!/bin/bash
cd /cygdrive/d/RealTimeFinancialAnomalyDetection/kafka_2.13-3.6.0/bin
./zookeeper-server-start.sh ../config/zookeeper.properties > /dev/null 2>&1 &