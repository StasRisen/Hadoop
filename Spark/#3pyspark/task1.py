#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark import SparkContext, SparkConf
import re
from pyspark.sql.types import *
from pyspark.sql import SparkSession

config = SparkConf().setAppName("task_1").setMaster("yarn").set("spark.ui.port", "5555")
sc = SparkContext(conf=config)
spark = SparkSession.builder.master("yarn").appName('task_1').getOrCreate()
rdd = sc.textFile("/data/wiki/en_articles_part")
rdd_stop = sc.textFile("/data/wiki/stop_words_en-xpo6.txt")

stop = rdd_stop.collect()
stop = sc.broadcast(stop)

def parseline(line):
    try:
        art_id, art = line.strip().lower().split("\t")
        text = re.sub("^\W+|\W+$", "", art).encode("utf-8")
        words = re.split(" ", text)
        return words
    except ValueError:
        return []
        
#Для подсчета слов (всех слов)
rdd_1 = rdd.map(parseline)
rdd_2 = rdd_1.flatMap(lambda x: x)
rdd_3 = rdd_2.filter(lambda x: len(x) > 0)
rdd_4 = rdd_3.map(lambda x: re.sub("[^A-Za-z0-9 \\s]","", x))
all_words = rdd_4.count()

#Для подсчета биграм (все биграммы)
def spaces_in_bigrams(words):
    cleanlist = []
    for i in range(0, len(words)):
        if len(words[i]) == 0:
            continue
        w = re.sub("[^A-Za-z0-9 \\s]","", words[i])
        cleanlist.append(w)
    return cleanlist
rdd_big_1 = rdd_1.map(spaces_in_bigrams)

def form_big(words):
    big = []
    bigrams = []
    for i in range(0, len(words)):
        big.append(words[i])
        if len(big) == 2:
            big = tuple(big)
            bigrams.append((big))
            big = [words[i]]
    return bigrams

rdd_big_2 = rdd_big_1.map(form_big)
rdd_big_3 = rdd_big_2.flatMap(lambda x: x)
all_bigrams = rdd_big_3.count()

#Подсчет биграм без стоп слов
def parse_stop(words):
    words_out = []
    for i in range(0, len(words)): 
        w = words[i]
        flag1 = False
        for j in range(0, len(stop.value)):
            if str(w) == str(stop.value[j]):
                flag1 = True
                j = len(stop.value) -1
        if flag1 == False:
            words_out.append(w)
    return words_out

rdd_fin_big = rdd_big_1.map(parse_stop) #вычищенные слова(словарь, надо flatMap делать)
rdd_final_bigrams = rdd_fin_big.map(form_big)

#Финальные вычисления над биграммами и словами(подсчет)
bigrams = rdd_final_bigrams.flatMap(lambda x: x).map(lambda x: (x[0] + '_' + x[1], 1))
big_finish = bigrams.reduceByKey(lambda a, b: a + b, 8).cache()

final_words = rdd_fin_big.flatMap(lambda x: x).map(lambda x: (x, 1))

rdd_words_count = final_words.reduceByKey(lambda a, b: a + b)
bigrams = big_finish.filter(lambda x: x[1] >= 500)
bigrams_rdd = bigrams.map(lambda x: (x[0], x[0].split('_')[0], x[0].split('_')[1], x[1], all_words, all_bigrams))

from pyspark.sql.types import *
schema_full = StructType(fields=[
    StructField("bigram", StringType()),
    StructField("word1", StringType()),
    StructField("word2", StringType()),
    StructField("bigram_sum", IntegerType()),
    StructField("word_all", IntegerType()),
    StructField("bigram_all", IntegerType())
])

from pyspark.sql.types import *
schema_word = StructType(fields=[
    StructField("word", StringType()),
    StructField("cnt", IntegerType())
])

bigrams_rdd_f = bigrams_rdd.map(lambda x: (x[0], x[1], x[2], int(x[3]), int(x[4]), int(x[5])))
word_counts_rdd = rdd_words_count.map(lambda x: (x[0], int(x[1])))

df_bigrams = spark.createDataFrame(bigrams_rdd_f, schema=schema_full)
df_words = spark.createDataFrame(word_counts_rdd, schema=schema_word)

df_bigrams.createOrReplaceTempView("bigrams")
df_words.createOrReplaceTempView("words")
df_full = spark.sql("SELECT b.*, w.cnt as cnt1, w2.cnt as cnt2\
                    FROM bigrams b\
                    LEFT JOIN words w ON w.word = b.word1\
                    LEFT JOIN words w2 ON w2.word = b.word2\
                    WHERE b.word1 != '' AND b.word2 != ''")
df_full.createOrReplaceTempView("full")

df_agg = spark.sql("SELECT f.*,\
-(LOG((f.bigram_sum/f.bigram_all)/((f.cnt1/f.word_all)*(f.cnt2/f.word_all))) / LOG(f.bigram_sum/f.bigram_all)) as NPMI \
FROM full f ORDER BY NPMI DESC")

df = df_agg.toPandas()

for i in range(0, len(df)):
    if i < 39:
        print(df.iloc[i,0])
