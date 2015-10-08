//Oct 2, 2015.
//Author: Jayant Gupta
//The code to analyze the data from the Bosch Appliance.

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
import org.apache.spark.broadcast.Broadcast;
import java.util.*;
import java.io.*;
import java.sql.Date;
import java.sql.Timestamp;

public class BoschAnalysis {

    private static String Base_Dir = "/home/bosch_data/data/";
    private static String dataToTest = Base_Dir + "dataToTest/";
    private static String data = Base_Dir + "2014-08-01/";

  public static void main(String[] args) {
    long startTime = System.currentTimeMillis();
    ArrayList<String> fileNames = new ArrayList<String>();
    listFilesForFolder(new File(Base_Dir), fileNames);
    // Initializing the Spark Conf.
    SparkConf conf = new SparkConf().setAppName("Analysing Bosch Data");
    // Initializing the spark context.
    // It is recommended to use a single sparkContext object instance.
    JavaSparkContext sc = new JavaSparkContext(conf);
    System.out.println(fileNames.size()); 
    for(String fileName : fileNames){
    //  DataFrame results = analyseData(sc, fileName);
    }
      DataFrame results = analyseData(sc, data + "665016929766.csv");
    System.out.println("SUCCESS!!");
    long endTime = System.currentTimeMillis();
    System.out.println("Total RunTime of the program : " + Long.toString(endTime - startTime) + " ms");
    /* 
    for(String name:names){
	System.out.println("--" + name + "--");
    }*/
  }
  // Uses Recursion. Needs to be avoided.
  // TODO Change recursion to for Loop.
  public static void listFilesForFolder(final File folder, ArrayList<String> fileNames) {
    for (final File fileEntry : folder.listFiles()) {
        if (fileEntry.isDirectory()) {
            listFilesForFolder(fileEntry, fileNames);
        } else {
//            System.out.println(fileEntry.getAbsolutePath());
	    fileNames.add(fileEntry.getAbsolutePath());
        }
    }
  }

  public static void extractData(DataFrame results){
      // The results of SQL queries the DataFrames and support all the normal RDD operations.
      // The columns of a row in the results can be accessed by ordial.
      List<String> names = results.javaRDD().map(new Function<Row, String>(){
          public String call(Row row){
	    return row.getString(0);
          }
      }).collect();
      return;
  }
  
  public static DataFrame analyseData(JavaSparkContext sc, String fileName){
    System.out.println( "----------------------\n" +fileName);
    // store the time of the program's start.
    long startTime = System.currentTimeMillis();
    SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
    DataFrame boschDataFrame = ConvertData(sc, sqlContext, fileName);
    // Store the time when the data conversion is complete.
    long convertTime = System.currentTimeMillis();
    System.out.println("Time taken to convert data : " + Long.toString(convertTime - startTime) + " Miliseconds"); 
    // Register the DataFrame as a table.
    DataFrame results = null;
    if(boschDataFrame != null){
      boschDataFrame.registerTempTable("bosch");
      //SQL can be run over RDDs that have been registered as tables.
      results= sqlContext.sql("SELECT Hot_water_switch FROM bosch");
      System.out.println(results.count());
    }
    // The results of SQL queries the DataFrames and support all the normal RDD operatios.
    // The columns of a row in the result can be acccessed by ordial.
    //List<String> names = results.javaRDD().map(new Function<Row, String>(){
    //  public String call(Row row){
    //	return "Date: " + row.getString(0);
    //  }
    //}).collect();
    long queryTime = System.currentTimeMillis();
    System.out.println("Time taken to query data : " + Long.toString(queryTime - convertTime) + " Miliseconds"); 
    return results;
  }
  
 // Convert .csv file to a MySQL schema. 
  public static DataFrame ConvertData(JavaSparkContext sc, SQLContext sqlContext, String fileName){

    // Load a text file and convert each line to a JavaBean.
    JavaRDD<String> bosch_data = sc.textFile(fileName);
    if(bosch_data.count() == 0 ) return null;

    // The schema is encoded in a string
    String [] fieldNames = bosch_data.first().split("\t");

  // Generate the schema based on the string of schema
    List<StructField> fields = new ArrayList<StructField>();
    fields.add(DataTypes.createStructField("Date", DataTypes.TimestampType, true));
//  timestamp_fields.add(DataTypes.createStructField("ID", DataTypes.FloatType, true));
    int length = fieldNames.length - 1;
    final Broadcast<Integer> schema_length = sc.broadcast(new Integer(length));
    System.out.println(length);

    //List<StructField> data_fields = new ArrayList<StructField>();
    for(int i = 2 ; i < fieldNames.length ; i++){
      fields.add(DataTypes.createStructField(fieldNames[i], DataTypes.FloatType, true));
    }
    for(int i=0;i<fieldNames.length;i++){
//	    System.out.println(fieldNames[i]);
    }
    StructType schema = DataTypes.createStructType(fields);
    // Convert records of the RDD (people) to Rows.
    JavaRDD<Row> rowRDD = bosch_data.map(
      new Function<String, Row>(){
	public Row call(String record) throws Exception{
	  String [] fields = record.split("\t");
	    String [] temp = fields[0].split("\\.");
	    String timestamp = "";
	    if(temp.length >= 3){
              timestamp = temp[2].trim() + "-" + temp[1].trim() + "-" + temp[0].trim() + " " + fields[1];
	    }
	    else return null;
	    String [] mod_fields = new String[fields.length - 2];
	    for(int i = 2 ; i < fields.length ; i++){
	      mod_fields[i-2] = fields[i];
	    }
	    int len = schema_length.value();
//	    System.out.println(len);
	    Object [] obj_fields = new Object[len];
	    Object [] test = new Object[2];
	    try{
	      obj_fields[0] = new Timestamp(Timestamp.valueOf(timestamp).getTime());
	      test[0] = new Timestamp(Timestamp.valueOf(timestamp).getTime());
	    }catch(Exception E){
	      System.out.println("Catched!! " + timestamp);
	      obj_fields[0] = new Timestamp(0);
	      test[0] = new Timestamp(0);
	    }
	    for(int i=0 ; i< mod_fields.length ; i++){
	      if(mod_fields[i].length() > 0){
	        obj_fields[i+1] = new Float(Float.parseFloat(mod_fields[i]));
	      }
	    }
	    for(int i=mod_fields.length ; i < obj_fields.length; i++){
		    obj_fields[i]= null;
	    }
	    Object ID;
	    if(fields[2].length() == 0){
		    test[1] = null;
	    }
	    else{
		    test[1] = new Float(fields[2]);
	    }
	    try{
	     // return RowFactory.create(test);
	     // return RowFactory.create(Timestamp.valueOf(timestamp), ID);
	      return RowFactory.create(obj_fields);
	    }catch(Exception E){
	      return null;
	    }
        }
     });
     System.out.println("#Rows : " + Long.toString(rowRDD.count()));
     // Apply the schema to the RDD.
     DataFrame boschDataFrame = sqlContext.createDataFrame(rowRDD, schema);
     return boschDataFrame;     
  }
}
