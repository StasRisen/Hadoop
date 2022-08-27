#!/usr/bin/env python
import sys

current_key = None
sum_count = 0
second_text =''
for line in sys.stdin:
    try:
        key_main, key, count = line.strip().split('\t')
        if key_main == None:
            continue
        count = int(count)
    except ValueError:
        continue
    if current_key != key_main:
        if current_key:
            print("{}\t{}\t{}".format(current_key, sum_count, second_text))
        sum_count = 0
        second_text = ''
        current_key = key_main

    sum_count +=count
    second_text += str(key) + ':' + str(count) + ';'

if current_key:
    print("{}\t{}\t{}".format(current_key, sum_count, second_text))
