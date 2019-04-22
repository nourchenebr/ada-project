#!/bin/bash
spark-submit --master yarn --deploy-mode cluster --driver-memory 4G --num-executors 10 --executor-memory 4G --executor-cores 10 $1
