#!/usr/bin/env python3

import sys

for line in sys.stdin:
    fields = line.split()
    fields[1] = str(fields[1]).replace('.ru/', '.com/')
    print("\t".join(fields))
