#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Utilitarian script to upload contents of a json file into mongo database collection

NOTE: in order to get pymongo in the path, you need to source the reqmgr2ms init.sh script
source /data/srv/current/sw/slc7_amd64_gcc630/cms/reqmgr2ms/0.4.5.pre3/etc/profile.d/init.sh
"""
from __future__ import print_function, division

import json
import sys

import pymongo


def main():
    myClient = pymongo.MongoClient("mongodb://ms-output-mongo.dmwm:8230/")
    myDB = myClient["msOutDB"]
    collections = ["msOutRelValColl", "msOutNonRelValColl"]

    for coll in collections:
        myCol = myDB[coll]
        
        fileName = "/data/user/import/{}.json".format(coll)
        with open(fileName) as fObj:
            fileData = json.load(fObj)
        
        print("Inserting {} documents into the DB collection: {}".format(len(fileData), coll))
        try:
            myCol.insert_many(fileData, ordered=False)
        except pymongo.errors.BulkWriteError as bwe:
            print("Skipped some duplicate entries listed below...")
            for error in bwe.details['writeErrors']:
                print(error['errmsg'])
    
    myClient.close()
    
    print("Done!")


if __name__ == '__main__':
    sys.exit(main())
