#!/usr/bin/env bash

OUT_DIR="out_maprd_2_svi"
NUM_REDUCERS=8

hdfs dfs -rm -r -skipTrash ${OUT_DIR}* > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
	-D mapreduce.job.name="mapreducetask2" \
	-D mapreduce.job.reduces=$NUM_REDUCERS \
	-files mapper.py,reducer.py,combiner.py \
	-mapper mapper.py \
	-combiner combiner.py \
	-reducer reducer.py \
	-input /data/wiki/en_articles \
	-output ${OUT_DIR}.tmp > /dev/null

hdfs dfs -rm -r -skipTrash ${OUT_DIR} > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
	-D stream.num.map.output.key.fields=2 \
	-D mapreduce.job.name="mapreducetask2 step2" \
        -D mapreduce.job.reduces=1 \
        -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
    	-D mapreduce.partition.keycomparator.options='-k2,3nr -k1,1r' \
	-files mapper2.py,reducer2.py\
        -mapper mapper2.py \
        -reducer reducer2.py \
        -input ${OUT_DIR}.tmp \
        -output ${OUT_DIR}.tmp2 > /dev/null

# Remove previous results
hdfs dfs -rm -r -skipTrash ${OUT_DIR} > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D stream.num.map.output.key.fields=2 \
    -D mapreduce.job.reduces=1 \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
    -D mapreduce.partition.keycomparator.options='-k2,2nr' \
    -mapper cat \
    -reducer cat \
    -input ${OUT_DIR}.tmp2 \
    -output ${OUT_DIR} > /dev/null



	hdfs dfs -cat ${OUT_DIR}/part-0000* | head -10
