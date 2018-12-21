package com.sxt.java.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import scala.actors.threadpool.Arrays;

import java.util.List;

public class SparkStreamForeachRDD {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();

        conf.setMaster("local[2]").setAppName("stream");

        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaReceiverInputDStream<String> dStream = streamingContext.socketTextStream("node01", 7777);

        JavaPairDStream<String, Integer> pairDStream = dStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

//        pairDStream.print();

        pairDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> pairRDD) throws Exception {
                /**
                 * 特点：rdd外部的代码 ，是在driver端执行的，每隔batchinterval都会执行一次。
                 * 例子： 动态改变广播变量（黑名单)
                 *
                 */

//                SparkContext context = pairRDD.context();
//
//                io...
//                List list = null;
//                final Broadcast<List> broadcast = context.broadcast(list);

                System.out.println("huhu------------------huhu");

                /**
                 * rdd可以继续转换，但是切记要有触发算子。
                 */
                pairRDD.filter(new Function<Tuple2<String, Integer>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Integer> v1) throws Exception {
//                        List value = broadcast.value();
                        System.out.println(v1 + " -------------------");
                        Integer count = v1._2;
                        if(count>=2){
                            return true;
                        }else{
                            return false;
                        }

                    }
                }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
                    @Override
                    public void call(Tuple2<String, Integer> tuple2) throws Exception {
                        System.out.println(tuple2);
                    }
                });
            }
        });

        streamingContext.start();

        streamingContext.awaitTermination();

    }
}
