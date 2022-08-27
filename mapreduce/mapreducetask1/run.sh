#!/usr/bin/env bash

OUT_DIR="out_maprd2"
NUM_REDUCERS=8

hdfs dfs -rm -r -skipTrash ${OUT_DIR}* > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapreduce.job.name="mapreducetask1" \
    -D mapreduce.job.reduces=$NUM_REDUCERS \
    -files mapper.py,reducer.py \
    -mapper mapper.py \
    -reducer reducer.py \
    -input /data/ids \
    -output ${OUT_DIR} > /dev/null

hdfs dfs -cat ${OUT_DIR}/part-0000* | head -50
