#!/usr/bin/python
# -*- coding:utf-8 -*-
import anydbm
import zlib
import logging
logging.basicConfig(level=logging.INFO)

db=anydbm.open('togetter.dbm')
cnt=0
for k in db.keys():
    logging.info(k)
    cnt+=1
    if cnt>10:
        break

    
