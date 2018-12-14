package com.sxt.scala.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Operator_filter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("filter")
    val sc =  new SparkContext(conf)
    val lines = sc.textFile("./words.txt")
    val result = lines.filter { _.indexOf("Spark") >= 0 }
    result.foreach { println}
    sc.stop()
  }
}