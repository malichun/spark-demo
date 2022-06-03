package com.atguigu.bigdata.spark.core.req

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Req1_HotCategoryTop10Analysis3 {
    def main(args: Array[String]): Unit = {
        // TODO: Top10热门品类

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
        val sc = new SparkContext(sparkConf)

        // 1. 读取原始的日志数据
        val actionRDD = sc.textFile("datas/user_visit_action.txt")

        // (品类ID, (点击数量, 下单数量, 支付数量))
        actionRDD.flatMap{ action =>
            val datas = action.split("_")
            if(datas(6) != "-1"){
                List((datas(6), (1,0,0)))
            }else if(datas(8) != "null"){
                datas(8).split(",").map(id => (id, (0,1,0)))
            }else if(datas(10) != "null"){
                datas(10).split(",").map(id => (id, (0,0,1)))
            }else{
                Nil
            }
        }
            .reduceByKey((t1,t2) => (t1._1+t2._1, t1._2+t2._2, t1._3+t2._3))
            .sortBy(_._2,false)
            .take(10)
            .foreach(println)

        sc.stop()

    }
}
