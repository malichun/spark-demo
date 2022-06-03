package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4))

        // val i = rdd.reduce(_ + _)

        var sum = 0
        rdd.foreach(num => {
            sum += num
        })

        println("sum="+sum) // 0, executor端的数据没有返回到driver端

        println(sum)

        sc.stop()
    }

}
