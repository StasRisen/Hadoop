#!/usr/bin/env python3
from pyspark import SparkContext, SparkConf
import re
from pyspark.sql.types import *
from pyspark.sql import SparkSession


config = SparkConf().setAppName("task_2").setMaster("yarn").set("spark.ui.port", "5555")
spark_context = SparkContext(conf=config)
spark = SparkSession.builder.master("yarn").appName('task_2').getOrCreate()

rdd = spark_context.textFile("/data/twitter/twitter_sample.txt")
rdd2 = rdd.map(lambda x: x.split('\t'))

rdd3 = rdd2.map(lambda x: (int(x[0]), int(x[1])))
rdd4 = rdd3.map(lambda x: (int(x[1]), int(x[0])))

schema = StructType(fields=[
    StructField("user", IntegerType()),
    StructField("follower", IntegerType())
])

reverse_schema = StructType(fields=[
    StructField("follower", IntegerType()),
    StructField("user", IntegerType())
])

df = spark.createDataFrame(rdd3, schema=schema)
forward_df = spark.createDataFrame(rdd4, schema=reverse_schema)

full_df = df.createOrReplaceTempView("orig")
full_df = spark.sql("Select * FROM orig WHERE user = 12")
f_df = forward_df
i = 1

user_prev = 'user_' + str(i-1)
f_prev = 'follower_' + str(i-1)
newColumns = [user_prev, f_prev]
full_df = full_df.toDF(*newColumns)

user = 'user_' + str(i)
follower = 'follower_' + str(i)
newColumns = [follower, user]
f_df = f_df.toDF(*newColumns)

while True:
    user_prev = 'user_' + str(i-1)
    f_prev = 'follower_' + str(i-1)

    user = 'user_' + str(i)
    follower = 'follower_' + str(i)
    newColumns = [follower, user]
    f_df = f_df.toDF(*newColumns)

    full_df = full_df\
        .join(f_df, full_df[user_prev] == f_df[follower], "left").drop(full_df[f_prev])

    full_df.createOrReplaceTempView("base")

    full_df = spark.sql("select * from base where {0} IS NOT NULL".format(user))
    full_df.createOrReplaceTempView("base")

    k = spark.sql("select {0} from base where {0} = 34 AND user_0 = 12".format(user)).count()
    i += 1
    if k > 0:
        df = full_df.drop(full_df[follower])
        df.createOrReplaceTempView("base")
        df = spark.sql("select distinct * from base where {0} = 34 AND user_0 = 12".format(user))
        df = df.distinct().toPandas()

        path = ''
        for index in range(0, i):
            if (index == i-1):
                path += str(df.iloc[0, index])
                print(path)
            else:
                path += str(df.iloc[0, index]) + ','
        break
