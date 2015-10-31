//Q1: How many different appliances do we have?
//Run command: mongo Query1.js
connection = new Mongo();
db = connection.getDB("boschData");
var collectionName = param1.toString();
//aggregation pipeline for the query
pipeline = [
	{"$group": {"_id": "$ApplianceId"}},
    	{"$group" : {"_id": "$ApplianceId", "count": {"$sum":1}}}
];

//run query
db.collection(collectionName).aggregate(pipeline).forEach(printjson);
