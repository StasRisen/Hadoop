#!/usr/bin/env python
import sys
from random import randint

for line in sys.stdin:
    try:
        random_number = randint(1, 8)
        print ("{}\t{}".format(random_number, line))
    except ValueError as e:
        print('Fatal ERROR')
        continue
