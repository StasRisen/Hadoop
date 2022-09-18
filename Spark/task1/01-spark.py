# -*- coding: utf-8 -*-

from pyspark import SparkContext, SparkConf
import re

config = SparkConf().setAppName("task_&").setMaster("yarn").set("spark.ui.port", "5555")
spark_context = SparkContext(conf=config)

rdd = spark_context.textFile("/data/wiki/en_articles_part")

def parseline(line):
    try:
        art_id, art = line.strip().lower().split("\t")
        text = re.sub("^\W+|\W+$", "", art).encode("utf-8")
        text = re.sub("[^A-Za-z\\s]", "", text)
        words = re.split(" ", text)
        return words
    except ValueError:
        return []

def bigrams(words):
    bigrams = []
    for i in range(0, (len(words)-2)):
        if str(words[i]) == "narodnaya" and words[i+1] != '':
            pair = u'_'.join((words[i], words[i+1])).encode('utf-8')
            bigrams.append((pair))
    return bigrams

rdd1 = rdd.map(parseline)
rdd2 = rdd1.map(bigrams)
rdd3 = rdd2.filter(lambda x: len(x))
rdd4 = rdd3.flatMap(lambda x: x)
rdd5 = rdd4.map(lambda x: (x, 1))
rdd6 = rdd5.reduceByKey(lambda a, b: a + b)
rdd7 = rdd6.sortBy(lambda a: a[0], ascending=True)
rdd8 = rdd7.collect()

for word, cnt in rdd8:
    print(str(word.decode("utf8") + " " + str(cnt)))
