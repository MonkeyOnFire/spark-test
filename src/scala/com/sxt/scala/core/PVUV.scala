package com.sxt.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Layne on 2018/5/4.
  */
object PVUV {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("sortByKey")
    val sc = new SparkContext(conf)
    val records = sc.textFile("data/pvuvdata")

    //pv
    records.map(x=>{
      val fields = x.split("\t")
      (fields(5),1)
    }).reduceByKey(_+_).foreach(println)

    //uv
    records.map(x=>{
      val fields = x.split("\t")
      (fields(5),fields(0))
    }).groupByKey().foreach(x=>{
      println(x._1+":"+x._2.toSet.size)
    })
  }
}
