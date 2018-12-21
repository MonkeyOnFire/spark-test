package com.sxt.java.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import scala.actors.threadpool.Arrays;

public class SparkStreamTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();

        conf.setMaster("local[2]").setAppName("stream");

        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaReceiverInputDStream<String> dStream = streamingContext.socketTextStream("node01", 7777);

        JavaDStream<String> wordDstream = dStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                String[] split = s.split(" ");
                return Arrays.asList(split);
            }

        });

        JavaPairDStream<String, Integer> pairDStream = wordDstream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        JavaPairDStream<String, Integer> resultDstream = pairDStream.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        resultDstream.print();

        streamingContext.start();

        streamingContext.awaitTermination();


        streamingContext.stop();





    }
}
