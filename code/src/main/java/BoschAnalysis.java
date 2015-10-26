//Oct 2, 2015.
//Author: Jayant Gupta
//The code to analyze the data from the Bosch Appliance.
//
// Updated Oct 25, 2015
//
/* Java Libraries */
import java.util.*;
import java.io.*;
import java.sql.Date;
import java.sql.Timestamp;

/* Spark Libraries */
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.broadcast.Broadcast;

/* Hadoop Libraries */
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
	
public class BoschAnalysis {
	private static String Base_Dir = "hdfs://cn1:9000/hdfs/dataset/data/";
	private static String dataToTest = Base_Dir + "dataToTest/";
	private static String data = Base_Dir + "2014-08-01/";
	private static String one_file = dataToTest + "oneFile";
	private static String three_file = dataToTest + "3files";
	private static String ten_file = dataToTest + "10files";
	private static String quarter_file = dataToTest + "25files";
	private static String fifty_file = dataToTest + "50files";
	private static String triquarter_file = dataToTest + "75files";
	private static String cent_file = dataToTest + "100files";
	private static ArrayList<String> sets = new ArrayList<String>();

	private static long nRows = 0;
  public static void main(String[] args)throws Exception {
		sets.add(ten_file);
		sets.add(quarter_file);
		sets.add(fifty_file);
		sets.add(triquarter_file);
		sets.add(cent_file);
		for(String my_path: sets){
			System.out.println(my_path);
			long startTime = System.currentTimeMillis();
			SparkConf conf = new SparkConf().setAppName("Analysing Bosch Data");
			final JavaSparkContext sc = new JavaSparkContext(conf);
			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus [] status = fs.listStatus(new Path(my_path));
			long loopStartTime = System.currentTimeMillis();
			for(int i = 0 ; i< status.length ; i++){
				analyseData(sc, status[i].getPath().toString());
				long loopEndTime = System.currentTimeMillis();
				System.out.print("Time taken after loop #" + Integer.toString(i) + " : " + Long.toString(loopEndTime - loopStartTime) + " ms for #rows: "); 
				System.out.println(nRows);
			}
			System.out.println("SUCCESS!!");
			long endTime = System.currentTimeMillis();
			System.out.println("Total RunTime of the program : " + Long.toString(endTime - startTime) + " ms");
			sc.stop();
			System.out.println("===============================================================");
		}
  }
  
	public static void analyseData(JavaSparkContext sc, String fileName){
//    System.out.println( "----------------------\n" +fileName);
    long startTime = System.currentTimeMillis(); // store the time of the program's start.
    SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
    DataFrame boschDataFrame = ConvertData(sc, sqlContext, fileName);
    long convertTime = System.currentTimeMillis();// Store the time when the data conversion is complete.
    System.out.println("Time taken to convert data : " + Long.toString(convertTime - startTime) + "ms"); 
		System.out.println("#rows in table: " + Long.toString(boschDataFrame.count()));
		nRows += boschDataFrame.count();
    if(boschDataFrame != null){
			ArrayList<String>queries = new ArrayList<String>();
			queries.add("SELECT * FROM bosch ORDER BY timestamp");
			queries.add("SELECT MIN(timestamp) FROM bosch");
			queries.add("SELECT MIN(timestamp) FROM bosch");
			queryMaker(sqlContext, boschDataFrame, queries);
    }
  }
  
	//Perform queries and output resource usage.
	public static void queryMaker(SQLContext sqlContext, DataFrame boschDataFrame, ArrayList<String> queries){
		boschDataFrame.registerTempTable("bosch"); // Register the DataFrame as a table.
		for(String query:queries){
			DataFrame results = null;
			long queryStartTime = System.currentTimeMillis();
			results = sqlContext.sql(query); 
			long queryEndTime = System.currentTimeMillis();
			System.out.print(query);
			System.out.print(":" + Long.toString(queryEndTime - queryStartTime) + "ms"); 
			System.out.println(" : #results: " + Long.toString(results.count()));
		}
	}
  
 // Convert .csv file to a MySQL schema. 
  public static DataFrame ConvertData(JavaSparkContext sc, SQLContext sqlContext, String fileName){
    JavaRDD<String> bosch_data = sc.textFile(fileName);// Load a text file and convert each line to a JavaBean.
		if(bosch_data.count() == 0 ) return null;
//		System.out.println("Convert Data: " + Long.toString(bosch_data.count()));
    String [] fieldNames = bosch_data.first().split("\t");// The schema is encoded in a string

  // Generate the schema based on the string of schema
    List<StructField> fields = new ArrayList<StructField>();
    fields.add(DataTypes.createStructField("timestamp", DataTypes.TimestampType, true));
    int length = fieldNames.length - 1;
    final Broadcast<Integer> schema_length = sc.broadcast(new Integer(length));
//    System.out.println("Convert Data: " + Integer.toString(length));
    for(int i = 2 ; i < fieldNames.length ; i++){
      fields.add(DataTypes.createStructField(fieldNames[i], DataTypes.FloatType, true));
    }
    StructType schema = DataTypes.createStructType(fields);
    // Convert records of the RDD (people) to Rows.
		JavaRDD<Row> rowRDD = bosch_data.map(
				new Function<String, Row>(){
				public Row call(String record) throws Exception{
					return transform(record, schema_length.value());
				}
				});
		rowRDD = rowRDD.filter(
				new Function<Row, Boolean>(){
					public Boolean call(Row record)throws Exception{
						return (record!=null);
						}});
     System.out.println("#Rows : " + Long.toString(rowRDD.count()));
 		 DataFrame boschDataFrame = null;
		 try{
	     boschDataFrame = sqlContext.createDataFrame(rowRDD, schema); // Apply the schema to the RDD.
		 }catch(Exception E){
			 System.out.println("ERROR: Cannot create DataFrame\n");
		 }
     return boschDataFrame;     
  }
	// Modifies the bosch data and creates ROW object.
	public static Row transform(String record, int len){
		String [] fields = record.split("\t");
		String [] temp = fields[0].split("\\.");
		String timestamp = "";
		if(temp.length == 3){
			timestamp = temp[2].trim() + "-" + temp[1].trim() + "-" + temp[0].trim() + " " + fields[1];
		}
		else return null;
		Object [] obj_fields = new Object[len];
		try{
			obj_fields[0] = Timestamp.valueOf(timestamp);
		}catch(Exception E){
			obj_fields[0] = new Timestamp(0);
		}
		for(int i=2 ; i < fields.length ; i++){
			if(fields[i]!=null && fields[i].length() > 0){
				obj_fields[i-1] = new Float(Float.parseFloat(fields[i]));
			}
			else{
				obj_fields[i-1] = null;
			}
		}
		try{
			return RowFactory.create(obj_fields); /* Returning the row here..*/
		}catch(Exception E){
			System.out.println(record);
			return null;
		}
	}	
}
