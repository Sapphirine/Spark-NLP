import java.util.Arrays;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;

import scala.Tuple2;

public class SimpleProject {
	public static void main(String[] args) {
		String logFile = "hdfs://localhost:8020/README.md"; // Should be some file on your system
		//    containsAorB(logFile);
		wordcount(logFile);
	}
	public static void containsAorB(String path){ 

		SparkConf conf = new SparkConf().setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logData = sc.textFile(path).cache();

		long numAs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) { return s.contains("a"); }
		}).count();

		long numBs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) { return s.contains("b"); }
		}).count();

		System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
	}

	public static void wordcount(String path){ 

		SparkConf conf = new SparkConf().setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> file = sc.textFile(path).cache();

		JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String s) { return Arrays.asList(s.split(" ")); }
		});
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) { return new Tuple2<String, Integer>(s, 1); }
		});
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer a, Integer b) { return a + b; }
		});
		System.out.println(counts.collect());
		//	    	counts.saveAsTextFile("hdfs://..."); 
	}
}