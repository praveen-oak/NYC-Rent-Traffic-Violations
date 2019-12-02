#!/usr/bin/env python

import sys

for line in sys.stdin:
    (key, value) = line.strip().split("\t")
    key = key[:7]
    if(int(key[:4]) < 2017 or int(key[:4]) > 2018 or int(key[-2:]) < 1 or int(key[-2:]) > 12):
        continue;
    dict = {"Manhattan": "NY", "Brooklyn": "BK", "Queens": "Q", "Bronx": "BX", "Staten Island": "ST"}
    if value in dict.keys():
        print("%s\t%s" % (key, dict.get(value)))
