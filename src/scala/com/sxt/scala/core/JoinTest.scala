package com.sxt.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Layne on 2018/5/3.
  */
object JoinTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("spark://bigdata01:7077").setAppName("count")
      .setJars(List("C:\\Users\\Sadrm\\Desktop\\bigdata03\\spark\\spark-test\\out\\artifacts\\spark_test_jar\\spark-test.jar"))
    val sc = new SparkContext(conf)

    val empRDD = sc.parallelize(List(
      ("userid01","lisi","m","4000","depid01"),
      ("userid02","zhangsan","f","3000","depid01"),
      ("userid03","wangwu","m","3500","depid02"),
      ("userid04","laowang","m","3200","depid03")
    ),5)

    val depRDD = sc.parallelize(List(
      ("depid01","销售部"),
      ("depid02","市场部"),
      ("depid04","市场部")
    ),5)


    val empMapRDD = empRDD.map(x=>{
      (x._5,x)
    })

    val depMapRDD = depRDD.map(x=>{
      (x._1,x)
    })

    println("内连接")
    empMapRDD.join(depMapRDD,2)/*.collect()*/.foreach(println)
    println("左外连接")
    empMapRDD.leftOuterJoin(depMapRDD).collect().foreach(println)
    println("右外连接")
    empMapRDD.rightOuterJoin(depMapRDD).collect().foreach(println)
    println("全外连接")
    empMapRDD.fullOuterJoin(depMapRDD).collect().foreach(println)
    empMapRDD.cache()
    sc.stop()

  }

}
