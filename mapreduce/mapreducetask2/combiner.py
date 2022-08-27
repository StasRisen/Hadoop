#!/usr/bin/env python

import sys

current_key = None
sum_count = 0

for line in sys.stdin:
    try:
        key, count = line.strip().split('\t')
        count = int(count)
    except ValueError:
        continue
    if current_key != key:
        print("{}\t{}".format(current_key, sum_count))
        sum_count = 0
        current_key = key
    sum_count +=count

if current_key:
    print("{}\t{}".format(key, sum_count))
