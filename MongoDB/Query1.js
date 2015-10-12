//Q1: How many different appliances do we have?
//Run command: mongo Query1.js
connection = new Mongo();
db = connection.getDB("dataset");
//aggregation pipeline for the query
pipeline = [
	{"$group": {"_id": "$ApplianceId"}},
    	{"$group" : {"_id": "$ApplianceId", "count": {"$sum":1}}}
];


db.system.profile.drop();
db.runCommand({profile: 2})
//run query
db.testColl.aggregate(pipeline).forEach(printjson);
db.runCommand({profile: 0})
//report time taken
print(db.system.profile.findOne().millis)