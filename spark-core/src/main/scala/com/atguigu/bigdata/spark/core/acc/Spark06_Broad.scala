package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

// 自定义累加器
object Spark06_Broad {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
        val sc = new SparkContext(sparkConf)

        val rdd1 = sc.makeRDD(List(
            ("a",1),("b",2),("c",3)
        ))


        val map = mutable.Map(("a",4),("b",5),("c",6))
        // 封装广播变量
        val bc = sc.broadcast(map)

        rdd1.map{
            case (w, c) => {
                // 访问广播变量
                val l = bc.value.getOrElse(w, 0)
                (w,(c, l))
            }
        }.collect.foreach(println)


        sc.stop()
    }

}
