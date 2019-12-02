#!/usr/bin/env python

import sys

dict = {}
with open('taxi+_zone_lookup.csv') as f:
    for line in f:
        line_split = line.strip().replace('"', '').split(",")
        dict[line_split[0]] = line_split[1]

for line in sys.stdin:
    values = line.strip().split(",")
    if len(values) != 17:
        continue
    if not values[1] or not values[2] or not values[7] or not values[8]:
        continue
    if(values[7] in dict.keys()):
        print("%s\t%s" % (values[1], dict.get(values[7])))
    if(values[8] in dict.keys()):
        print("%s\t%s" % (values[2], dict.get(values[8])))
