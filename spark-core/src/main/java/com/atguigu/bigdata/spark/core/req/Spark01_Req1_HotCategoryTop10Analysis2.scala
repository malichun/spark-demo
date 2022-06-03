package com.atguigu.bigdata.spark.core.req

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Req1_HotCategoryTop10Analysis2 {
    def main(args: Array[String]): Unit = {
        // TODO: Top10热门品类

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis2")
        val sc = new SparkContext(sparkConf)

        // Q: actionRDD 重复使用
        // Q: cogroup性能可能较低

        // 1. 读取原始的日志数据
        val actionRDD = sc.textFile("datas/user_visit_action.txt")

        // 1.点击 2.下单, 3.支付

        actionRDD.flatMap[((Int, String),Int)](action => {
            val datas = action.split("_")
            if(datas(6) != "-1"){
                List(((1,datas(6)),1))
            }else if(datas(8) != "null"){
                val datas = action.split("_")
                val cids = datas(8).split(",")
                cids.map(id => ((2,id), 1))
            }else if(datas(10)!="null"){
                val datas = action.split("_")
                val cids = datas(10).split(",")
                cids.map(id => ((3,id), 1))
            }else{
                List[((Int, String),Int)]()
            }
        })
            .reduceByKey(_+_)
            .map{case ((actionType, categoryId), count) =>
                (categoryId,(actionType, count))
            }
            .groupByKey()
            .map(t => {
                var clickCnt = 0
                var orderCnt =0
                var payCnt = 0

                val categoryId = t._1
                val iter = t._2
                iter.foreach{
                    case (1,cnt) => clickCnt = cnt
                    case (2,cnt) => orderCnt = cnt
                    case (3, cnt) => payCnt = cnt
                }
                (categoryId,(clickCnt,orderCnt, payCnt))
            })
            .sortBy(_._2,false)
            .take(10)
            .foreach(println(_))

        sc.stop()

    }
}
