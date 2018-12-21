package com.sxt.java.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkStreamTransform {

    public static void main(String[] args) {


        SparkConf conf = new SparkConf();

        conf.setMaster("local[2]").setAppName("stream");

        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaReceiverInputDStream<String> dStream = streamingContext.socketTextStream("node01", 7777);


        JavaDStream<String> transform = dStream.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaRDD<String> v1) throws Exception {

                /**
                 *
                 * rdd外部是在dirver端运行。这里的代码 每隔batchinterval执行一次。。
                 */


                JavaRDD<String> map = v1.map(new Function<String, String>() {
                    @Override
                    public String call(String v1) throws Exception {
                        return v1 + " ~~~~~~~~~~";
                    }
                });
                return map;
            }
        });

        JavaDStream<String> map = dStream.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                return v1 + " ~~~~~~~~~~";
            }
        });

        map.print();

//        transform.print();
        streamingContext.start();

        streamingContext.awaitTermination();

    }
}
