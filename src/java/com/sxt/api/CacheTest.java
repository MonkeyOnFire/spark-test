package com.sxt.api;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by Layne on 2018/5/2.
 */
public class CacheTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("CacheTest");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("./NASA_access_log_Aug95");

        lines = lines.cache();
        long startTime = System.currentTimeMillis();
        long count = lines.count();
        long endTime = System.currentTimeMillis();
        System.out.println("共"+count+ "条数据，"+"初始化时间+cache时间+计算时间="+
                (endTime-startTime));

        long countStartTime = System.currentTimeMillis();
        long countrResult = lines.count();
        long countEndTime = System.currentTimeMillis();
        System.out.println("共"+countrResult+ "条数据，"+"计算时间="+ (countEndTime-
                countStartTime));

        jsc.stop();

    }

}
