package com.spark.firstapp;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class EstimatingPi {

	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark Count").setMaster("local"));

		List<Integer> data = new ArrayList<Integer>(); 
		for (int i = 1; i < 1000000; i++) {
			data.add(i);
		}
		
		long count = sc.parallelize(data).filter(new Function<Integer, Boolean>() {
			  public Boolean call(Integer i) {
			    double x = Math.random();
			    double y = Math.random();
			    return x*x + y*y < 1;   // By pythogoras theorem, distance from centre i.e. if point lie within circle
			  }
			}).count();

		System.out.println("Pi is roughly " + 4.000000 * count / 1000000 );
		
	}

	
	
	
}
