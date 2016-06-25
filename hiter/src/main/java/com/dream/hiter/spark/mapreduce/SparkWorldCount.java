package com.dream.hiter.spark.mapreduce;

import java.util.Arrays;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.SparkConf;

import scala.Tuple2;

/**
 * spark-submit --master spark://eb174:7077 --name JavaWordCountByHQ --class com.hq.JavaWordCount
 * --executor-memory 1G --total-executor-cores 2 ~/test/WordCount.jar
 * hdfs://eb170:8020/user/ebupt/text
 * 
 * @author GIT
 *
 */
public class SparkWorldCount {
  /**
   * this .
   * @param args xx
   */
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("worldCount").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> textFile = sc.textFile("D:\\hello.properties");// hdfs://
    JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
      public Iterable<String> call(String s) {
        return Arrays.asList(s.split("\\s+"));
      }
    });

    JavaPairRDD<String, Integer> pairs =
        words.mapToPair(new PairFunction<String, String, Integer>() {
          public Tuple2<String, Integer> call(String s) {
            return new Tuple2<String, Integer>(s, 1);
          }
        });

    JavaPairRDD<String, Integer> counts =
        pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
          public Integer call(Integer a, Integer b) {
            return a + b;
          }
        });

    System.out.println(counts.collect());
    // counts.saveAsTextFile("hdfs://...");

  }

}
