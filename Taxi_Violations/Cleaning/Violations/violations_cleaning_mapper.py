#!/usr/bin/env python

import sys

for line in sys.stdin:
    values = line.strip().split(",")
    if not values[4] or not values[15]:
        continue
    print("%s\t%s" % (values[4], values[15]))
