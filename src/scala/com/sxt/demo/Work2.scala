package com.sxt.demo

import org.apache.spark.{SparkConf, SparkContext}

object Work2 {



  case class OnlineItemLog(userid:Long,
                           eventid:Int,
                           itemid:String,
                           pay_num:String,
                           pay_type:String,
                           sys_version:String,
                           soft_version:String,
                           dtime:String,
                           ip:String,
                           year:String,
                           month:String,
                           day:String,
                           area:String
                          ){
    def toCsvString(): String ={
      userid+","+
        eventid+","+
        itemid+","+
        pay_num+","+
        pay_type+","+
        sys_version+","+
        soft_version+","+
        dtime+","+
        ip+","+
        year+","+
        month+","+
        day+","+
        area
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("WeatherStatistic")
    val sc = new SparkContext(conf)
    val linesRdd = sc.textFile("data/online_item_log.csv")
    var r1 = linesRdd.map(line=>{
      /**
      假定 192、172、122、168 开头的 IP 分别为代表北
      京、上海、广州和深圳
        */
      val a = Map(
        "192"->"beijing",
        "172"->"上海",
        "122"->"广州",
        "168"->"深圳"
      )
      var tmp = line.split(",")
      OnlineItemLog(tmp(0).toLong,
        tmp(1).toInt,
        tmp(2),
        tmp(3),
        tmp(4),
        tmp(5),
        tmp(6),
        tmp(7),
        tmp(8),
        tmp(9),
        tmp(10),
        tmp(11),
        a.getOrElse(tmp(9).substring(0,3),"null")
      )
    })
    val r1_cache = r1.cache()
    // 1. 统计 pv  订单
    val c1 = r1_cache.filter(x=>x.eventid==1 && x.dtime>"2017-11-04" && x.dtime<"2017-11-18").count()
    val c2 = r1_cache.filter(x=>x.eventid==2 && x.dtime>"2017-11-04" && x.dtime<"2017-11-18").count()
     println("pv:" +c1)
     println("订单:" + c2)
    // 2. 订单量前十的商品
    val c3 = r1_cache.filter(x=>x.eventid==2 && x.dtime>"2017-12-04" && x.dtime<"2017-12-18").cache()
//      .map(x=>(x.itemid,x.pay_num.toDouble))

    // 构建每个商品的订单量和支付金额的元祖，用于同时计算各商品总订单量与总支付金额（注：直接使用浮点来计算会出现失去精度问题，转Int处理）
    val item_kv = c3.map(x=>(x.itemid,(1,x.pay_num.replace(".","").toInt)))
    val it_count_pay = item_kv.reduceByKey((x,y)=>{
      (x._1+y._1,x._2+y._2)
    }).cache()

    // 对每个商品基于订单量进行排序,并取Top 10
    val it_top10 = it_count_pay.sortBy(_._2._1,false).take(10)
    it_top10.foreach(println)

    // 计算前十商品的总支付金额
    val pay_total_top10 = it_count_pay.map(_._2._2).sum()
    println("前十商品的总支付金额: " + pay_total_top10/10)


    //3.计算双十二前后7天的所有商品的总销售额
    val it_pay_total =  c3.map(x=>x.pay_num.replace(".","").toInt).sum()
    println("双十二前后7天的所有商品的总销售额: " + it_pay_total/10)

    /**
    4.统计在 2017 年一整年内各个软件版本在各个平台下不同各个地区的用户使用情况（使
    用次数，有一次记录即算一次使用）（提示：系统版本、软件版本、根据 IP 归属地解析结果
    分组）
     */

    val c5 = r1_cache.filter(x=>  x.dtime>"2017-01-01" && x.dtime<"2018-01-01").cache()

    c5.groupBy(_.area).map(_._1).foreach(print)

    val use_count = c5.map(
      x=>((x.sys_version,x.soft_version,x.area),1)
    ).reduceByKey(_+_)
    println("各个软件版本在各个平台下不同各个地区的用户使用情况 :" )
    use_count.foreach(println)


    /**
      *
      * 统计 2017-11-04 至 2017-11-18 和 2017-12-04 至 2017-12-18 双十一与双十二期间，
        各个平台下 各个版本的 男女用户 使用量（一次记录算一次使用）分布情况
      *
      */

    val c6 = r1_cache.filter(x=>  (x.dtime>"2017-11-04" && x.dtime<"2017-11-18") || (x.dtime>"2017-12-04" && x.dtime<"2017-12-18")).cache()

    val ur = sc.textFile("data/user_register.csv").map(_.split(","))

    val en_kv = c6.map(x=>(x.userid.toString,x))
    val ur_kv = ur.map(x=>(x(0),x(2)))

    val en_and_sex = en_kv.join(ur_kv).map(x=>{
      ((x._2._1.sys_version,x._2._1.soft_version,x._2._2),1)
    })

    val use_and_sex = en_and_sex.reduceByKey(_+_)
    println("各个平台下 各个版本的 男女用户 使用量（一次记录算一次使用）分布情况")
    use_and_sex.foreach(println)

    sc.stop()
  }
}
