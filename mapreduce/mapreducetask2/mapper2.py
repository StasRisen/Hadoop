#!/usr/bin/env python
import sys
import re

for line in sys.stdin:
    try:
        key_main, key, count = line.strip().split('\t')
        if key_main == None:
            continue
        print("{}\t{}\t{}".format(key_main, key, count))
    except ValueError as e:
        continue
    
