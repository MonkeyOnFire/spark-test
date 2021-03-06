package com.sxt.test

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Operator_coalesce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("coalesce")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(1,2,3,4,5,6),4)
    val rdd2 = rdd1.mapPartitionsWithIndex((partitionIndex,iter)=>{
      val list = new ListBuffer[String]()
      while(iter.hasNext){
        list += "rdd1 PartitonIndex : "+ partitionIndex+",value :"+iter.next()
      }
      list.iterator
    })
    rdd2.foreach { println }
    val rdd3 = rdd2.coalesce(5, true)
    val rdd4 = rdd3.mapPartitionsWithIndex((partitionIndex,iter)=>{
      val list = new ListBuffer[String]()
      while(iter.hasNext){
        list += "coalesce PartitionIndex :"+partitionIndex+",value:"+iter.next()
      }
      list.iterator
    })
    rdd4.foreach { println}
    
    sc.stop()
  }
}