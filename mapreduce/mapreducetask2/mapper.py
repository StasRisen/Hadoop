#!/usr/bin/env python
import sys
import re

for line in sys.stdin:
    try:
        art_id, text = line.strip().split('\t')
    except ValueError as e:
        continue
    words = re.split('[ \t\n\r\f\v\.,;!?:"@#()]', text)
    for word in words:
        word = re.split('[^A-Za-z\\s]', word)
        for wrd in word:
            if len(wrd) < 3:
                continue
            print("{}\t{}".format(wrd.lower(),1))
