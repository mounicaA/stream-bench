//Q2: Get the days of data, start date and end date for a given appliance.
//Shell command to run this script:
//mongo --eval "var param1=<applianceId>" Query2.js
//example: mongo --eval "var param1=107146362662" Query2.js

connection = new Mongo();
db = connection.getDB("boschData");

//query start
param = param1.toString();
var collectionName = param2.toString();
var cursor = db.getCollection(collectionName).find({"ApplianceId" : param}, { "_id" : 0, "TS" : 1, "ApplianceId" : 1}).sort({'TS':1}).limit(1);
var startDate, endDate;
while(cursor.hasNext()){
        var obj = cursor.next();
        startDate = obj.TS;
        }

var cursor2 = db.getCollection(collectionName).find({"ApplianceId": param}, {"_id":0, "TS":1, "ApplianceId":1}).sort({'TS':-1}).limit(1);

while(cursor2.hasNext()){
        var obj2 = cursor2.next();
        endDate = obj2.TS;
        }

print("End date of data: ", endDate);
print("Start date of data: ", startDate);
var diffDays = parseInt((endDate-startDate)/(1000*60*60*24))
print("Number of days of data: ", diffDays);

//end of query
