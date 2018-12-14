package com.sxt.scala.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
/**
 * 该函数将RDD中的元素和这个元素在RDD中的索引号（从0开始）组合成（K,V）对
 */
object zipWithIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("zipWithIndex")
    val sc = new SparkContext(conf)
    val rdd1 = sc.makeRDD(Array((1,"a"),(2,"b"),(3,"c")))
    val result = rdd1.zipWithIndex()
    result.foreach(println)
    sc.stop()
    
  }
}