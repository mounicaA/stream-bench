#Usage
#python hiveQueries.py <table name>

import pyhs2
import pyhive
import dateutil.parser as dparser

conn = pyhs2.connect(host='localhost',
                   port=10000,
                   authMechanism="PLAIN",
                   user='huser',
                   password='test',
                   database='testdb')
cur = conn.cursor()
import timeit
import pandas as pd

#table to query from for table specific queries
tableName = sys.argv[1]

#Query 1: How many different appliances we have?
start = timeit.default_timer()
query = 'show tables'
cur.execute(query)
tables = cur.fetch()

print ("No. of appliances = %s" % len(tables))
for i in tables:
    print i

stop = timeit.default_timer()
runtime = stop - start

print("Runtime for Query 1 is: %s" %runtime)

#Query 2: Get the number of days of Data and start and end date for the given specific appliance

start = timeit.default_timer()

query2_2 = """select to_date(from_unixtime(date2)) as start_date from (select distinct unix_timestamp(Date, 'dd.MM.yy') as date2 from """+tableName+""") t2 order by start_date limit 2"""
cur.execute(query2_2)

s = cur.fetch()

query2_3 = """select to_date(from_unixtime(date2)) as end_date from (select distinct unix_timestamp(Date, 'dd.MM.yy') as date2 from """+tableName+""") t2 order by end_date DESC limit 2"""
cur.execute(query2_3)

e = cur.fetch()

end_date = dparser.parse(str(e[0]), fuzzy=True)
start_date = dparser.parse(str(s[1]), fuzzy=True)

print("Start Date: %s" % start_date)
print("End Date: %s" % end_date)
delta = end_date - start_date

print("Total number of days: %s" %delta)

stop = timeit.default_timer()
runtime = stop - start

print("Runtime for Query 2 is: %s" %runtime)

# Query3: Display the error and specific start and end date of the error, and the display_code
start = timeit.default_timer()

query3_1 = """select current_fault_display_code, Date, Time, Operating_status_Error_locking from """+tableName+""" where Operating_status_Error_locking = '1'"""
cur.execute(query3)

results = cur.fetch()

for result in results:
    startDate = result[1]
    startTime = result[2]

    query3_2 = """select current_fault_display_code, Date, Time, Operating_status_Error_locking from """+tableName+""" where Operating_status_Error_locking = '0' and from_unixtime(Date) >= from_unixtime(startDate) and from_unixtime(Time) > from_unixtime(startTime)"""
    
    for i in cur.fetch():
        print i

stop = timeit.default_timer()
runtime = stop - start

print("Runtime for Query 3 is: %s" %runtime)

#Query 4: Summary of a column - normal mean, median, count, stddev, min, max, 25%, 75%
start = timeit.default_timer()

query4 = """select Date, Time, Supply_temperature_primary_flow_temperature from """+tableName+""" where Supply_temperature_primary_flow_temperature <> ''"""
cur.execute(query4)

results = cur.fetch()

stop = timeit.default_timer()
runtime = stop - start

print("Runtime for Query 4 is: %s" %runtime)

