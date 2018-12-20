package com.sxt.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Iskandar on 2018/12/14.
  */
//object weather {
//  def main(args: Array[String]): Unit = {
//    var conf = new SparkConf()
//    conf.setMaster("local").setAppName("weather")
//    var sc = new SparkContext(conf)
//    var rdd1  = sc.textFile("data/weather")
//    //val rdd2 = rdd1.map(line=>{var arr = line.split(" ")(1).split("\t");(arr(2).toString , arr(0).toString)})
//
//    var rdd2 = rdd1.map{ x => (x.split(" ")(0) -> x.split("\t")(1))}
//    var rdd3 = rdd2.reduceByKey{(x,y) => if(x>y) x else y}
//
//    var rdd4 = rdd3.map{ x => (x._1.substring(0,7) -> x._1.substring(0,10)+" -> "+x._2)}
//    var rdd5 = rdd4.groupBy(_(0)).sortBy(_(2))
//    var rdd7 = rdd5.take(2)
//
//    rdd7.foreach{println}
//  }
//}
