package one.two.three;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class ErrorSearch {
	
	public static void main(String[] args){
		
		//SparkConf conf = new SparkConf().setMaster("local").setAppName("CountingSheep").set("spark.executor.memory", "1g");
		//SparkContext sc = new SparkContext(conf);

		
		String logFile = "C:\\spark\\localhost.2015-01-14.log"; // Should be some file on your system
	    SparkConf conf = new SparkConf().setAppName("Simple Application");		
		JavaSparkContext spark = new JavaSparkContext(conf);
		JavaRDD<String> textFile = spark.textFile(logFile);
		JavaRDD<String> errors = textFile.filter(new Function<String, Boolean>() {
		  public Boolean call(String s) { return s.contains("ERROR"); }
		});
		// Count all the errors
		errors.count();
		// Count errors mentioning MySQL
		errors.filter(new Function<String, Boolean>() {
		  public Boolean call(String s) { return s.contains("MySQL"); }
		}).count();
		// Fetch the MySQL errors as an array of strings
		errors.filter(new Function<String, Boolean>() {
		  public Boolean call(String s) { return s.contains("MySQL"); }
		}).collect();
	}
}
