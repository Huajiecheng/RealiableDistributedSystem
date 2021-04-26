#!/usr/bin/python3

import json
import os

def read_config(fname):
    conf = None
    with open(fname) as jsonconf:
        conf = json.load(jsonconf)
        jsonconf.close()
    
    return conf

def update_config(conf, fname):
    with open(fname, 'w') as jsonconf:
        json.dump(conf, fname, indent=4)
        jsonconf.close()


