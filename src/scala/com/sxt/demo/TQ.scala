package com.sxt.demo


import org.apache.spark.{SparkConf, SparkContext}
object TQ {
  def main(args: Array[String]): Unit = {
      var conf = new SparkConf()
      conf.setMaster("local").setAppName("TQ")
      var sc = new SparkContext(conf)

     var lines = sc.textFile("tq")
      var pair = lines.map(x =>(x.split(" ")(0),x.split("\t")(1)))
      var group =pair.reduceByKey((x,y) => if(x >= y) x else y)
    //var group = pair.groupByKey(0)
      //var re =group.reduceByKey((x,y) => if(x >= y) x else y )
      var pair2 = group.map( x =>(x._2,x._1,x._1.substring(0,7))).sortBy(x => x._1,false)
      //var group2 =pair2.groupByKey(0)
      var result =pair2.groupBy(x =>x._3)
      var result2 =result.map(item=>{
        item._2.take(2)
      }).foreach(println)
      //var result2 =result.takeOrdered(1)
    //var result =pair2.sortBy(,ascending = false)
    //pair2.foreach(println)
      sc.stop()
  }


}
