package com.spark.firstapp;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark Count").setMaster("local"));
		JavaRDD<String> file = sc.textFile("input.txt");
		
		JavaRDD<String> words = file.flatMap( s -> { return Arrays.asList(s.split(" ")); } );
		
		JavaPairRDD<String, Integer> pairs = words.mapToPair( key -> { return new Tuple2<String, Integer>(key, 1)  ;} );
		
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey( (a,b) -> {return a+b ; });
		
		counts.saveAsTextFile("output/output2.txt");
	}

}
