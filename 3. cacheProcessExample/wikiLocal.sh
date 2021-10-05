#!/bin/bash
cd /home/hadoop/$1/bin
           ./spark-submit --class org.iscas.motivationWordCount  \
           --conf spark.driver.memory=512m \
           --conf spark.executor.memory=512m \
           /home/hadoop/cacheProcessExample/cacheProcessExample-1.0.jar local[8] $2
