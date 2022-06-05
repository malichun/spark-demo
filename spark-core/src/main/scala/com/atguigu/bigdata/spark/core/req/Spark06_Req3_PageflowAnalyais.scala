package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

//在需求一的基础上，增加每个品类用户session的点击统计
object Spark06_Req3_PageflowAnalyais {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
        val sc = new SparkContext(sparkConf)
        val actionRDD = sc.textFile("datas/user_visit_action.txt")

        val actionDataRDD = actionRDD.map(action => {
            val datas = action.split("_")
            UserVisitAction(
                datas(0),
                datas(1).toLong,
                datas(2),
                datas(3).toLong,
                datas(4),
                datas(5),
                datas(6).toLong,
                datas(7).toLong,
                datas(8),
                datas(9),
                datas(10),
                datas(11),
                datas(12).toLong
            )
        })
        actionDataRDD.cache()

        // TODO 计算分母
        val pageidToCountMap = actionDataRDD.map(action => {
            (action.page_id, 1)
        }).reduceByKey(_ + _).collect().toMap

        // TODO 计算分子
        // 根据session进行分组
        val sessionRDD = actionDataRDD.groupBy(_.session_id)
        // 分组后, 根据访问时间进行排序(升序)
        val dataRdd = sessionRDD.mapValues(iter => {
            val sortList = iter.toList.sortBy(_.action_time)
            val flowIds = sortList.map(_.page_id)
            val pageflowIds = flowIds.zip(flowIds.tail)
            pageflowIds.map(t => (t, 1))
        })
            .map(_._2)
            .flatMap(list => list)
            .reduceByKey(_+_)


        // TODO 计算单挑转换率
        // 分子 / 分母
        dataRdd.foreach{
            case ((pageid1, pageid2), sum ) =>
                val lon = pageidToCountMap.getOrElse(pageid1, 0L)
//                println(s"页面[${pageid1}]跳转到页面${pageid2}单跳转换率为:${sum.toDouble / lon}")
        }


        sc.stop()
    }

    //用户访问动作表
    case class UserVisitAction(
                                  date: String,//用户点击行为的日期
                                  user_id: Long,//用户的ID
                                  session_id: String,//Session的ID
                                  page_id: Long,//某个页面的ID
                                  action_time: String,//动作的时间点
                                  search_keyword: String,//用户搜索的关键词
                                  click_category_id: Long,//某一个商品品类的ID
                                  click_product_id: Long,//某一个商品的ID
                                  order_category_ids: String,//一次订单中所有品类的ID集合
                                  order_product_ids: String,//一次订单中所有商品的ID集合
                                  pay_category_ids: String,//一次支付中所有品类的ID集合
                                  pay_product_ids: String,//一次支付中所有商品的ID集合
                                  city_id: Long
                              )//城市 id
}
