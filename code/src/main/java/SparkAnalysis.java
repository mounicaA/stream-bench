/* SparkAnalysis.java */
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
// Import factory methods provided by DataTypes.
import org.apache.spark.sql.types.DataTypes;
//import StructType and StructField 
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
//import Row.
import org.apache.spark.sql.Row;
// import RowFactory
import org.apache.spark.sql.RowFactory;
// import DataFrame
import org.apache.spark.sql.DataFrame;
// import SQLContext
import org.apache.spark.sql.SQLContext;

import java.util.*;

public class SparkAnalysis {
  public static void main(String[] args) {
//    String logFile = "/home/jayantg/oneFile/101313054542.csv"; // Should be some file on your system
    SparkConf conf = new SparkConf().setAppName("Analysing Spark Data");
    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

//   Load a text file and convert each line to a JavaBean.
    JavaRDD<String> people = sc.textFile("/home/jayantg/spark/examples/src/main/resources/people.txt");

// The schema is encoded in a string
    String schemaString = "name age";

//Generate the schema based on the string of schema
    List<StructField> fields = new ArrayList<StructField>();
    for(String fieldName: schemaString.split(" ")){
      fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
    }
    StructType schema = DataTypes.createStructType(fields);

    //Convert records of the RDD (people) to Rows.
    JavaRDD<Row> rowRDD = people.map(
      new Function<String, Row>(){
	public Row call(String record) throws Exception{
	  String[] fields = record.split(",");
	  //return RowFactory.create(fields[0], fields[1].trim());
	  return RowFactory.create(fields);
        }
     });
     //Apply the schema to the RDD.
     DataFrame peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema);
     // Register the DataFrame as a table.
     peopleDataFrame.registerTempTable("people");
     //SQL can be run over RDDs that have been registered as tables.
     DataFrame results= sqlContext.sql("SELECT name FROM people");

     // The results of SQL queries the DataFrames and support all the normal RDD operatios.
     // The columns of a row in the result can be acccessed by ordial.
     List<String> names = results.javaRDD().map(new Function<Row, String>(){
       public String call(Row row){
	return "Name: " + row.getString(0);
      }
     }).collect();

     for(String name:names){
	System.out.println("--" + name + "--");
     }
  }
}
