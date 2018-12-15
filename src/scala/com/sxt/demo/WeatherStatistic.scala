package com.sxt.demo

import org.apache.spark.{SparkConf, SparkContext}

object WeatherStatistic {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("WeatherStatistic")
    val sc = new SparkContext(conf)
    val linesRdd = sc.textFile("data/weather")
    // 切分原始日志 →（1949-10-01,14:21:02,34c）
    val r1 = linesRdd.map(_.split(Array[Char](' ','\t')))
    // 1. 取每天的最高温度,按天（第一个元素）分组，得到如下：
    val r1_1 = r1.groupBy(_(0))
    // 分组升序排序（sortBy），再反转（reverse）变为降序取第一个（take） —— scala中sortBy不支持升序排序
    val r1_2 = r1_1.map(x=>x._2.toList.sortBy(_(2)).reverse.take(1))
    // 取Key → 1949-10-01
    val r1_3 = r1_2.map(_(0))
    // 2. 取每月最高的温度, 构建 (月,完整数据)的KV结构（1949-10,(1949-10-01,14:21:02,38c)）
    val r2_1 = r1_3.map(x=>(x(0).substring(0,7),x))
    val r2_2 = r2_1.groupBy(_._1)
    // 每组根据温度升序排序，并反转为降序取前二
    val r2_3 = r2_2.map(x=>x._2.toList.sortBy(_._2(2)).reverse.take(2))
    // 3. 打印结果
    val r3 = r2_3.map(_.map(_._2))
    r3.foreach(_.foreach(x=> {
        println(x(0)+" "+x(1)+" "+x(2)+" ")
      })
    )
    sc.stop()
  }
}
