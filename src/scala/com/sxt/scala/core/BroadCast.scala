package com.sxt.scala.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object BroadCast {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("brocast")
    val sc = new SparkContext(conf)
    val list = List("hello xasxt")
    val broadCast = sc.broadcast(list)
    val lineRDD = sc.textFile("data/words.txt")
    lineRDD.filter { x => broadCast.value.contains(x) }.foreach { println}
    sc.stop()
  }
}