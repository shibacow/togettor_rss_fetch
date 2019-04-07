#!/usr/bin/python
# -*- coding:utf-8 -*-
import anydbm
import zlib
import logging
logging.basicConfig(level=logging.INFO)

db=anydbm.open('togetter.dbm')
s=db.keys()
s=s[:100]
for a in sorted(s):
    c=db[a]
    sz=len(c)
    k=zlib.decompress(c)
    logging.info(k[:1000])
    
