connection = new Mongo();
db = connection.getDB("dataset");
collectionName=$1
var cursor = db.collection(collectionName).aggregate(
   [
      { $group: { _id: null, count: { $sum: 1 } } }
   ]
);


while(cursor.hasNext()){
        var obj = cursor.next();
        printjson(obj);
}
