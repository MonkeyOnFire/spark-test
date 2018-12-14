package com.sxt.api;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Iterator;

/**
 * Created by Layne on 2018/5/4.
 */
public class TopN {

    public static void main(String[] args) {
        SparkConf conf;
        conf = new SparkConf()
                .setMaster("local")
                .setAppName("TopOps");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> linesRDD = sc.textFile("data/scores.txt");

        JavaPairRDD<String, Integer> pairRDD = linesRDD.mapToPair(new PairFunction<String, String, Integer>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String str) throws Exception {
                String[] splited = str.split(" ");
                String clazzName = splited[0];
                Integer score = Integer.valueOf(splited[1]);
                return new Tuple2<String, Integer> (clazzName,score);
            }
        });

        pairRDD.groupByKey().foreach(new
                                             VoidFunction<Tuple2<String,Iterable<Integer>>>() {

                                                 /**
                                                  *
                                                  */
                                                 private static final long serialVersionUID = 1L;

                                                 @Override
                                                 public void call(Tuple2<String, Iterable<Integer>> tuple) throws Exception {
                                                     String clazzName = tuple._1;
                                                     Iterator<Integer> iterator = tuple._2.iterator();

                                                     Integer[] top3 = new Integer[3];

                                                     while (iterator.hasNext()) {
                                                         Integer score = iterator.next();

                                                         for (int i = 0; i < top3.length; i++) {
                                                             if(top3[i] == null){
                                                                 top3[i] = score;
                                                                 break;
                                                             }else if(score > top3[i]){
                                                                 for (int j = 2; j > i; j--) {
                                                                     top3[j] = top3[j-1];
                                                                 }
                                                                 top3[i] = score;
                                                                 break;
                                                             }
                                                         }
                                                     }
                                                     System.out.println("class Name:"+clazzName);
                                                     for(Integer sscore : top3){
                                                         System.out.println(sscore);
                                                     }
                                                 }
                                             });

    }
}
