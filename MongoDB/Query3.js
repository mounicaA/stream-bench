//Q3: When the locking flag (‘Operating_System_Error_Locking’) goes from 0 to 1, it means that there is an error. 
//A display code shows the //specific error. Find out if any of the appliances faced errors. 
//If there were any errors, find the specific start and end of the error and also find the display code.

//Run command: mongo Query3.js

connection = new Mongo();
db = connection.getDB("dataset");

db.runCommand({profile:0});
db.system.profile.drop();
db.runCommand({profile:2});

//query start

var cursor = db.testColl.find({ "Operating_status:_Error_Locking": "1"},{"_id":0, "Current_fault_display_code":1, "Operating_status:_Error_Locking":1, "Date":1, "ApplianceId":1});

var errorStartTime, applianceId;

while(cursor.hasNext()){
	var obj = cursor.next();
	errorStartTime = obj.Date;
	applianceId = obj.ApplianceId;
	print("Start of Operating Status Error");
	printjson(obj);
    
	var cursor2 = db.testColl.find({  "Operating_status:_Error_Locking": "0", "Date":{"$gt": errorStartTime}, "ApplianceId": applianceId}, {"_id":0, "Current_fault_display_code":1, "Operating_status:_Error_Locking":0, "Date":1, "ApplianceId":1});
  
	while(cursor2.hasNext()){
		var obj2 = cursor2.next();
		print("End of Operating Status Error");
		printjson(obj2);
	}
}

//end of query

db.runCommand({profile: 0});
print("Runtime of the query in ms: ", db.system.profile.findOne().millis);