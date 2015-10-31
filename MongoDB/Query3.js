//Q3: When the locking flag (‘Operating_System_Error_Locking’) goes from 0 to 1, it means that there is an error. 
//A display code shows the //specific error. Find out if any of the appliances faced errors. 
//If there were any errors, find the specific start and end of the error and also find the display code.

//Run command: mongo Query3.js

connection = new Mongo();
db = connection.getDB("dataset");

//query start
var collectionName = param1.toString();
var cursor = db.getCollection(collectionName).find({ "Operating_status:_Error_Locking": "1"},{"_id":0, "Current_fault_display_code":1, "Operating_status:_Error_Locking":1, "TS":1, "ApplianceId":1});

var errorStartTime, applianceId;

while(cursor.hasNext()){
	var obj = cursor.next();
	errorStartTime = obj.TS;
	applianceId = obj.ApplianceId;
	print("Start of Operating Status Error");
	printjson(obj);
    
	var cursor2 = db.getCollection(collectionName).find({  "Operating_status:_Error_Locking": "0", "TS":{"$gt": errorStartTime}, "ApplianceId": applianceId}, {"_id":0, "Current_fault_display_code":1, "Operating_status:_Error_Locking":1, "TS":1, "ApplianceId":1});
  
	while(cursor2.hasNext()){
		var obj2 = cursor2.next();
		print("End of Operating Status Error");
		printjson(obj2);
	}
}

//end of query
