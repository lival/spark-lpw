#!/bin/bash
dataPath=/home/hadoop/cacheProcessExample/testData.txt
for k in 2
do
 for sparkPath in  'lru/spark-2.2.3' 'lpw/spark-2.2.3' 
 do
       sh wikiLocal.sh ${sparkPath} ${dataPath} >./logs/${sparkPath}_${k}_local.log 2>&1
    done
 done
    echo "cacheProcessData Local"
