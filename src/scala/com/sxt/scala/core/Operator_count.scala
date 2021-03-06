package com.sxt.scala.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Operator_count {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("count")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("data/words.txt")
    val result = lines.count()
    println(result)
    sc.stop()
  }
}