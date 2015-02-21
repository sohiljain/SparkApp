package com.sears.spark.firstapp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
public class App 
{
    public static void main( String[] args )
    {
//        System.out.println( "Hello World!" );
    	
    	JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark Count").setMaster("local"));
    	JavaRDD<String> file = sc.textFile("input.txt");
    	JavaRDD<String> info = file.filter(s -> s.contains("INFO"));
    	System.out.println(info.count());
    	List<String> arr = info.filter(s -> s.contains("maven")).collect();   
    	System.out.println(arr);
//    	info.saveAsTextFile("output/output1.txt");
    	
    	/*   	errors.count();
    	
    	
    	
    	
    	// Count errors mentioning MySQL
    	errors.filter(new Function<String, Boolean>() {	
    	  public Boolean call(String s) { return s.contains("maven"); }
    	}).count();
    	// Fetch the MySQL errors as an array of strings
    	errors.filter(new Function<String, Boolean>() {
    	  public Boolean call(String s) { return s.contains("single"); }
    	}).collect();
 */   	

//    	JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark Count"));
//        final int threshold = Integer.parseInt(args[1]);
        
        // split each document into words
/*        JavaRDD<String> tokenized = sc.textFile(args[0]).flatMap(
          new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) {
              return Arrays.asList(s.split(" "));
            }
          }
        );
        
        // count the occurrence of each word
        JavaPairRDD<String, Integer> counts = tokenized.mapToPair(
          new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
              return new Tuple2<String, Integer>(s, 1);
            }
          }
        ).reduceByKey(
          new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer i1, Integer i2) {
              return i1 + i2;
            }
          }
        );
        
        // filter out words with less than threshold occurrences
        JavaPairRDD<String, Integer> filtered = counts.filter(
          new Function<Tuple2<String, Integer>, Boolean>() {
            public Boolean call(Tuple2<String, Integer> tup) {
              return tup._2() >= threshold;
            }
          }
        );
        
        // count characters
        JavaPairRDD<Character, Integer> charCounts = filtered.flatMap(
          new FlatMapFunction<Tuple2<String, Integer>, Character>() {
            @Override
            public Iterable<Character> call(Tuple2<String, Integer> s) {
              Collection<Character> chars = new ArrayList<Character>(s._1().length());
              for (char c : s._1().toCharArray()) {
                chars.add(c);
              }
              return chars;
            }
          }
        ).mapToPair(
          new PairFunction<Character, Character, Integer>() {
            @Override
            public Tuple2<Character, Integer> call(Character c) {
              return new Tuple2<Character, Integer>(c, 1);
            }
          }
        ).reduceByKey(
          new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
              return i1 + i2;
            }
          }
        );
        
        System.out.println(charCounts.collect());

 */   
    }
}
