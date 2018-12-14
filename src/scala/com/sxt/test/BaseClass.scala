//package com.sxt.test
//
//import com.sharing.Params
//import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession
//import org.apache.spark.sql.{SQLContext, SparkSession}
//import org.apache.spark.{SparkConf, SparkContext}
//
///**
// * Created by xia.jun on 16/8/8.
// */
//trait BaseClass {
//  /**
//   * define some parameters
//   */
////  @transient
//  var sc:SparkContext = null
//  implicit var sqlContext:SQLContext = null
//  val config = new SparkConf()
//  var spark:SparkSession = null
//
//
//  /**
//   * initialize global parameters
//   */
////  @transient
//  def init()={
//    spark = SparkSession.builder().config(config).enableHiveSupport().getOrCreate()
//    sc = spark.sparkContext
//    sqlContext = spark.sqlContext
//    }
//
//
//  /**
//   * this method do not complete.Sub class that extends BaseClass complete this method
//   */
//  @transient
//  def execute(params: Params)
//
//
//  /**
//   * release resource
//   */
////  @transient
//  def destroy()={
//    if(sc!=null) {
//      sqlContext.clearCache()
//      sc.stop()
//    }
//  }
//
//}
