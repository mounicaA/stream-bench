//Find the characteristics for a particular column (mean, min, max, count and standard deviation).
//run command: mongo Query4.js
connection = new Mongo();
db = connection.getDB("dataset");
db.runCommand({profile:0});
db.system.profile.drop();
db.runCommand({profile:2});

//query start

//Match on all the documents where supply temperature exists	
var matchQ =	{"$match":{
	"ApplianceId": "107146362662", 
	"Supply_temperature_(primary_flow_temperature)": {"$exists": "true"}
	}
	};
	
//Project only supply temperature
var projectTemp =	{"$project":{
	"Supply_temperature_(primary_flow_temperature)":1
	}
	};

//Calculate the stats min, max, average and count
var statsQ =	{
	"$group": {
	"_id":null, 
	"min": {$min: "$Supply_temperature_(primary_flow_temperature)"},
	"max": {$max: "$Supply_temperature_(primary_flow_temperature)"},
	"count": {$sum: 1},
	"average": {$avg: "$Supply_temperature_(primary_flow_temperature)"}
	}};

//run query
var result = db.testColl.aggregate(matchQ, projectTemp, statsQ);
var obj;

while(result.hasNext()){
	obj = result.next();
	printjson(obj);
	}

var average = obj.average;	

//Find the variance
var diffSquaredQ =	{
	"$project": {
	"diffSquared":{
			"$let":{
				"vars":{
					"diff":{"$subtract":["$Supply_temperature_(primary_flow_temperature)", average]},
				},
				"in":{"$multiply":["$$diff", "$$diff"]}
				}
			},
		}
	};
	
var varianceCalc =	{
	"$group":{
		"_id":null,
		"variance":{$avg: "$diffSquared"}
		}
	};

var result2 = db.testColl.aggregate(matchQ, projectTemp, diffSquaredQ, varianceCalc);
var obj2;

while(result2.hasNext()){
	obj2 = result2.next();
	}

var variance = obj2.variance;
//Calculate std dev from the variance
var stdDev = Math.sqrt(variance);

print("Standard Deviation: ", stdDev);

	
//end of query

db.runCommand({profile: 0});
print("Runtime of the query in ms: ", db.system.profile.findOne().millis);