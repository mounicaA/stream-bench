#Usage: python toMongo.py <path to folder containing the files to insert>
#Example: python /home/mounica/scripts/toMongo.py /home/mounica/data/dataToTest/3files/

#Requirements: 1. pymongo, 2. pandas python package

import pymongo
import csv
from cStringIO import StringIO
from datetime import datetime
import timeit
import collections
import bson
from bson import SON
from bson import code
from pymongo import ASCENDING, DESCENDING
import pandas as pd
import sys
import glob
import errno

try:
    conn = pymongo.MongoClient()
    print "Connected Successfully!"
except pymongo.errors.ConnectionFailure, e:
    print "Could not connect: %s" %e

db = conn.boschData

path = sys.argv[1]
path = path + "*.csv"
collection = db.test
files = glob.glob(path)

print(files)

for oneFile in files:
	try:
		print("Opening file: %s" % oneFile)
		with open(oneFile) as f:
			reader = csv.DictReader(f, delimiter='\t')
			print("Reading file: %s" % oneFile)
			folders = oneFile.split("/")
			applianceid = folders[-1]
			applianceid = applianceid.replace(".csv", "")
			print("Inserting records for appliance: %s" %applianceid)
			for row in reader:
			    RecordDate = row['Date']
			    RecordTime = row['Time']
			    DateFields = RecordDate.split('.')
			    TimeFields = RecordTime.split(':')
			    year = int(DateFields[2])
			    month = int(DateFields[1])
			    day = int(DateFields[0])
			    hour = int(TimeFields[0])
			    minute = int(TimeFields[1])
			    second = int(TimeFields[2])
			    d = datetime(year, month, day, hour, minute, second)
			    row['Date'] = d
			    del row['Time']
			    
			    row['Supply_temp__Positive_tolerance'] = row.pop('Supply_temp__Pos._tolerance')
			    row['Supply_temp__Negative_tolerance'] = row.pop('Supply_temp__Neg._tolerance')
			    row['ApplianceId'] = applianceid
			    #print(row)
			    entry = {k:v for k,v in row.iteritems() if ((v!='NULL') and (v!=''))}
			    #print(entry)
			    collection.insert(entry, w=0)
			print("Inserted all records!")
			#create a compound index on appliance id and datetime
			collection.create_index([("ApplianceId", ASCENDING), ("Date", ASCENDING)])
	except IOError as exc:
		if exc.errno != errno.EISDIR:
			raise


print(collection.find_one())
