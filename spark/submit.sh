#!/bin/bash

# script pwd
BASEDIR=$(dirname "$0")
cd $BASEDIR

REPERTOIRE=$1

# Job properties
APPNAME=fr.edf.dco.edma.EdmaDataQuality

# Getting job properties
jarfile=$(ls ../lib/DataQuality-*.jar | tail -1 |awk '{print $1}')
CHECKSCONFIGURATION=$(ls ../config/$REPERTOIRE/* | paste -d, -s)
CONFIGS=hdfs:///user/dco_app/usr/conf/hbase-site.xml,hdfs:///user/dco_app/usr/conf/core-site.xml

spark-submit \
  --jars hdfs:///user/dco_app/usr/lib/hdfs/HDFSLogger-2.6.0.jar \
  --files $CHECKSCONFIGURATION,$CONFIGS \
  --class fr.edf.dco.edma.edq.job.Launcher \
  --name $APPNAME \
  --master yarn-cluster \
  --queue dco_ma_edma \
  --num-executors 6 \
  --executor-cores 5 \
  --executor-memory 3G \
  --conf spark.yarn.maxAppAttempts=4 \
  --conf spark.yarn.am.attemptFailuresValidityInterval=1h \
  --conf spark.yarn.max.executor.failures=152 \
  --conf spark.yarn.executor.failuresValidityInterval=1h \
  --conf spark.task.maxFailures=8 \
  --conf spark.speculation=true \
  --conf spark.hadoop.fs.hdfs.impl.disable.cache=true \
 $jarfile
