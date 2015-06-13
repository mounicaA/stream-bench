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

try:
    conn = pymongo.MongoClient()
    print "Connected Successfully!"
except pymongo.errors.ConnectionFailure, e:
    print "Could not connect: %s" %e

db = conn.boschData
collection = db.appliances25
applid = sys.argv[2]

print(collection.find_one())

#Query 1 for select count(distinct applianceId) from table
start = timeit.default_timer()

pipeline = [
    {"$group": {"_id": "$ApplianceId"}},
    {"$group" : {"_id": "$ApplianceId", "count": {"$sum":1}}}
]

print(collection.aggregate(pipeline))

stop = timeit.default_timer()

runtime = stop-start
print ("The time taken for query 1 is: %s" % runtime)

#Query2: Get the number of days of Data and start and end date for the given specific appliance

start = timeit.default_timer()

start_date = collection.find({"ApplianceId": applid}, { "_id":0, "Date": 1, "ApplianceId":1}).sort("Date", ASCENDING).limit(1)

end_date = collection.find({"ApplianceId": applid}, {"_id":0, "Date": 1, "ApplianceId":1}).sort("Date", DESCENDING).limit(1)

s = {}

for i in start_date:
    s.update(i)

print(s)

e = {}

for i in end_date:
    e.update(i)

if bool(e) and bool(s):
	delta = e['Date'] - s['Date']
	print(delta)
	print(e)



stop = timeit.default_timer()

runtime = stop-start
print ("The time taken is for Query 2 is: %s" % runtime)

#Query 3: Display the error and specific start and end date of the error, and the display_code

start = timeit.default_timer()

cursor = collection.find(
    { "Operating_status:_Error_Locking": "1"
        
    },{
        "_id":0, "Current_fault_display_code":1, "Operating_status:_Error_Locking":1, "Date":1, "ApplianceId":1 
    })

for doc in cursor:
    result = doc
    errorStartTime = result['Date']
    applianceId = result['ApplianceId']
    
    print(result)
    
    cursor2 = collection.find(
    {  "Operating_status:_Error_Locking": "0", "Date":{"$gt": errorStartTime}, "ApplianceId": applianceId
     }, {
            "_id":0, "Current_fault_display_code":1, "Operating_status:_Error_Locking":1, "Date":1, "ApplianceId":1
        })
    
    for doc2 in cursor2:
        print doc2

stop = timeit.default_timer()

runtime = stop-start
print ("The time taken is for Query 3 is: %s" % runtime)

#Query4: Summary of a column - normal mean, median, count, stddev, min, max, 25%, 75%

start = timeit.default_timer()

cursor = collection.find({"ApplianceId": applid, "Supply_temperature_(primary_flow_temperature)": {"$exists": "true"}, },{"_id":0, "Date":1, "Supply_temperature_(primary_flow_temperature)":1})

RecordDate = []
SupTemp = []

for document in cursor:
    #print document.keys()
    RecordDate.append(document['Date'])
    SupTemp.append(document['Supply_temperature_(primary_flow_temperature)'])

docs = dict(zip(RecordDate, SupTemp))
df = pd.DataFrame(docs.items(), columns=['Date', 'SupTemp'])
df['SupTemp'] = df['SupTemp'].astype(float)

print(df['SupTemp'].describe())
    
stop = timeit.default_timer()

runtime = stop-start
print ("The time taken for Query 4 is: %s" % runtime)