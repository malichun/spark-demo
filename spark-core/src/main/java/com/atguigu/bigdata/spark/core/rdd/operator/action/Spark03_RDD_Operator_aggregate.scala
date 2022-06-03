package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_aggregate {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4), 2)

        // TODO - 行动算子
        // 10 + 13 + 17 = 40
        // aggregateByKey: 只会参与分区内计算
        // aggregate: 初始值会参与分区内计算,并且会参与分区间的计算
//        val result: Int = rdd.aggregate(10)(_ + _, _ + _)
        val result: Int = rdd.fold(10)(_ + _)
        println(result)

        sc.stop()

    }
}
