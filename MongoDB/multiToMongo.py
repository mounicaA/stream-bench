#Script to load files into mongodb
#Usage: python multiToMongo.py <path to folder containing the files to insert> <collection name>

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
from multiprocessing.pool import ThreadPool as Pool

pool_size = 5
pool = Pool(pool_size)


try:
	conn = pymongo.MongoClient()
	print "Connected Successfully!"
except pymongo.errors.ConnectionFailure, e:
	print "Could not connect: %s" %e

db = conn.boschData

path = sys.argv[1]
col_name = sys.argv[2]
path = path + "*.csv"
collection = getattr(db, col_name)
files = glob.glob(path)

def is_number(s):
	try:
		float(s)
		return True
	except (ValueError, TypeError) as e:
		return False


def worker(oneFile):
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
				if year < 2014:
					year = 2014
				month = int(DateFields[1])
				day = int(DateFields[0])
				hour = int(TimeFields[0])
				minute = int(TimeFields[1])
				second = int(TimeFields[2])
				d = datetime(year, month, day, hour, minute)
				row['TS'] = d
				del row['Date']
				del row['Time']

				row['Supply_temp__Positive_tolerance'] = row.pop('Supply_temp__Pos._tolerance')
				row['Supply_temp__Negative_tolerance'] = row.pop('Supply_temp__Neg._tolerance')
				row['ApplianceId'] = applianceid

				for k,v in row.items():
						if k != 'Date' and k != 'ApplianceId' and v != '' and is_number(v):
							if "status" not in k and v != '':
								row[k] = float(v)

				entry = {k:v for k,v in row.iteritems() if ((v!='NULL') and (v!=''))}
				collection.insert(entry, w=0)
			
			print("Inserted all records!")
			#create a compound index on appliance id and datetime
			
	except IOError as exc:
		if exc.errno != errno.EISDIR:
			raise



for oneFile in files:
	pool.apply_async(worker, oneFile)

collection.create_index([("ApplianceId", ASCENDING), ("Date", ASCENDING)])

pool.close()
pool.join()


print(collection.find_one())
