#!/usr/bin/env python
import sys
from random import randint

for line in sys.stdin:
    try:
        iterator = 1
        id_, id_value = line.strip().split('\t')
        text = id_value
        rand_value = randint(1,5)
        if (iterator < rand_value):
            for line1 in sys.stdin:
                id_, id_value = line1.strip().split('\t')
                text += ',' + str(id_value)
                iterator +=1
                if (iterator == rand_value):
                    break
        print('{}'.format(text))
    except ValueError as e:
        continue
