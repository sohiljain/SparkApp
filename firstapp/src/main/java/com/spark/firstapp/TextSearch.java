package com.spark.firstapp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.zookeeper.Op.Delete;

import scala.Tuple2;

/**
 * Hello world!
 *
 */
public class TextSearch 
{
	public static void main( String[] args )
	{
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark Count").setMaster("local"));
		JavaRDD<String> file = sc.textFile("input.txt");
		JavaRDD<String> info = file.filter(s -> s.contains("INFO"));
		info.saveAsTextFile("output/output1.txt");

		// Count lines with INFO
		long infoCount = info.count();
		System.out.println("InfoCount : " + infoCount);

		// Fetch strings with maven and INFO as a String List
		List<String> filteredStr = info.filter(s -> s.contains("maven")).collect();
		for (String str : filteredStr) {
			System.out.println(str);
		}
		
		/*        ********For JDK7 and below *************
		JavaRDD<String> errors = file.filter(new Function<String, Boolean>() {
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
		*/
	}
}
