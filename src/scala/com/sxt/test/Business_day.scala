//package com.sxt.test
//
//import java.text.SimpleDateFormat
//import java.util.{Properties, Date}
//
//import com.sharing.{Params, ParamsParseUtil}
//import com.sharing.utils.BaseClass
//import common.MySqlOps
//import org.apache.spark.sql.functions.{col, count}
//import org.apache.commons.dbutils.QueryRunner
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.{SparkConf, SparkContext}
//
//object Business_day extends BaseClass {
//
//
//  def main(args: Array[String]): Unit = {
//
//    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
//    val sc = new SparkContext(conf)
//    val sqlContext = new HiveContext(sc)
//    //    init()
//    //    ParamsParseUtil.parse(args) match {
//    //      case Some(p) => {
//    //        val params = p
//    //        execute(params = params)
//    //      }
//    //      case _ => {}
//    //    }
//    //    destroy()
//
//  }
//
//  //日期
//  def nowdate(): String = {
//    val now: Date = new Date()
//    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
//    val date = dateFormat.format(now)
//    return date.toString
//  }
//
//  override def execute(params: Params): Unit = {
//
//    //商家数
//    // 截止到当前日期商家总数
//    //  提取商家用户 商家数
//    val businessDF = sqlContext.sql("select  user_id , user_name  ,substring(from_unixtime(user_ctime + 8 * 60 * 60),0,10) as day_info from dw_mysql.ods_users as ods_users where userType = 1 ")
//
//    // 需求1 商家数 topics;users 需求1-10
//    // mysql自增主键
//    // val traceIDDF = sqlContext.sql("SELECT LAST_INSERT_ID() as currentId")
//
//    val resultDF = businessDF
//      .agg(count(col("user_id")))
//      .select("count(user_id)")
//
//    val insertSQL01 = "insert into  B_business_day(amount_bid) values (?) "
//
//    //需求2 商家在线商品总数统计 topics:topic_id amount
//    val topiconlineDF = sqlContext.sql(
//      """
//        |select topic_id , topic_status , topic_user_id as user_id , amount ,substring(from_unixtime(topic_ctime + 8 * 60 * 60),0,10) as topic_day_info
//        |from dw_mysql.topics as topics
//        |where  topic_status =0 and topic_type=2
//        |
//        |
//          """.stripMargin)
//
//    val topiccountDF = topiconlineDF.join(businessDF, Seq("user_id"))
//      .select("user_id", "user_name", "topic_id", "amount")
//
////    val topiccountDF002 = topiconlineDF.join(businessDF, Seq("user_id"))
////      .select( "topic_id")
//    val resultDF02 = topiccountDF
//      .agg(count(col("topic_id")))
//    val insertSQL02 = "insert into  B_business_day(online_topic) values (?) "
//
//
//    // 需求3  在线总库存 topics :amount
//    val resultDF03 = topiccountDF
//      .agg(count(col("amount")))
//
//    val insertSQL03 = "insert into  B_business_day(online_amount) values (?) "
//
//    //需求4 当日销售出商品数 根据订单表order_id  只要有订单的不管状态都算 i 限制当日
//    val orderDF = sqlContext.sql("select topic_user_id as user_id, order_id,orders_topic_id  ,order_price , order_fee , substring(from_unixtime(order_ctime + 8 * 60 * 60),0,10) as order_day_info  from dw_mysql.ods_user_orders as ods_user_orders  where substring(from_unixtime(order_ctime + 8 * 60 * 60),0,10) =" + nowdate())
//
//    val selltopicsDF = orderDF.join(businessDF, Seq("user_id"))
//      .select("user_id", "user_name", "order_day_info", "order_id", "order_price", "order_fee")
//
//    val resultDF04 = selltopicsDF
//      .agg(count(col("order_id")))
//
//    val insertSQL04 = "insert into  B_business_day(new_sellorder) values (?) "
//
//    //需求5  销售出总件数 user_orders:order_id 不限制时间
//    val orderDF02 = sqlContext.sql("select topic_user_id as user_id, order_id,orders_topic_id  ,order_price , order_fee , substring(from_unixtime(order_ctime + 8 * 60 * 60),0,10) as order_day_info  from dw_mysql.ods_user_orders as ods_user_orders ")
//    val sellamountDF = orderDF02.join(businessDF, Seq("user_id"))
//      .select("user_id", "user_name", "order_id")
//
//    val resultDF05 = sellamountDF
//      .agg(count(col("order_id")))
//    val insertSQL05 = "insert into  B_business_day(sell_orders) values (?) "
//
//    //需求6 当日新增商品数 topics:topic_id
//    val topiconlineDF02 = sqlContext.sql("select topic_id , topic_status , topic_user_id as user_id , amount ,substring(from_unixtime(topic_ctime + 8 * 60 * 60),0,10) as topic_day_info from dw_mysql.topics as topics where  topic_status =0 and topic_type=2 and substring(from_unixtime(topic_ctime + 8 * 60 * 60),0,10) = " + nowdate())
//
//    val topiccountDF02 = topiconlineDF02.join(businessDF, Seq("user_id"))
//      .select("user_id", "user_name", "topic_id", "amount","topic_day_info").registerTempTable("topiccountDF02")
//
//    topiconlineDF02.registerTempTable("topiconline02")
//
//    val newTopics = sqlContext.sql(
//      " select " +
//          " count(topic_id) as new_topics," +
//          "substring(now(), 1, 10) as day_p " +
//      " from " +
//          "topiconline02 " +
//      "where " +
//          "b.user_id = t02.user_id"
//    )
//
//    newTopics.show()
//
//    val insertSQL06 = "insert into  B_business_day(new_topics) values (?) "
//
//    //需求7 当日新增库存 topics:amount
//
//    val onlineAmountRs = sqlContext.sql(
//      " select " +
//        " count(amount) as online_amount," +
//        " substring(now(), 1, 10) as day_p " +
//      " from " +
//        "topiconline02 " +
//      "where " +
//        "b.user_id = t02.user_id" +
//      "group by " +
//        " user_id "
//    )
//
//    onlineAmountRs.show()
//
//    val temp_rs = newTopics.join(onlineAmountRs,Seq("day_p"))
//
//
//    val connectionProperties = new Properties()
//    val url = "jdbc:mysql://172.17.172.39:3306/sharing_bi"
//    val table = "B_business_day";
//    connectionProperties.setProperty("user", "root")// 设置用户名
//    connectionProperties.setProperty("password", "root")// 设置密码
//    temp_rs.write.mode("append").jdbc(url,table,connectionProperties)
//
//    val insertSQL07 = "insert into  B_business_day(new_amount,day_p) values (?,nowdate()) "
//
//    val insertSQL = insertSQL01+insertSQL02+insertSQL03+insertSQL04+insertSQL05+insertSQL06+insertSQL07
//    // val insertSQL = "insert into  B_business_day(amount_bid,online_topic,online_amount,new_sellorder,sell_orders,new_topics,new_amount,day_p) values (?,?,?,?,?,?,?,nowdate()) "
//
//    //写入mysql
//    resultDF.foreachPartition(partition => {
//      val dbUtil = MySqlOps("jdbc:mysql://172.17.172.39:3306/sharing_bi", "sharing_bi", "29KpwAviQGvZ")
//      partition.foreach(row => {
//        dbUtil.insert(insertSQL,row.getLong(0),row.getLong(1),row.getLong(2),row.getLong(3),row.getLong(4),row.getLong(5),row.getLong(6),row.getString(7))
//      })
//    })
//
//    println("sudooooooooo")
//
//
//  }
//  println("sudooooooooo33333333333")
//
//
//}
//
//
//
//
