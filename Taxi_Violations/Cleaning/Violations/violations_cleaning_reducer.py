#!/usr/bin/env python

import sys

for line in sys.stdin:
    (key, value) = line.strip().split("\t")
    key_split = key.split("/")
    if(len(key_split) != 3 or int(key_split[2]) < 2017 or int(key_split[2]) > 2018 or int(key_split[0]) < 1 or int(key_split[0]) > 12):
        continue
    key = key_split[2] + "-" + key_split[0]
    list = ["NY", "MN", "BK", "K", "BX", "Q", "QN", "ST"]
    dict = {"MN": "NY", "K": "BK", "QN": "Q"}
    if value in list:
        if value in dict.keys():
            print("%s\t%s" % (key, dict.get(value)))
        else:
            print("%s\t%s" % (key, value))
