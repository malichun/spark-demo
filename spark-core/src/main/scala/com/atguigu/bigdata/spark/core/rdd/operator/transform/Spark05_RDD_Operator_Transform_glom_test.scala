package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Transform_glom_test {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD( List(1,2,3,4), 2)

        // 分区内求最大值, 分区间求和
        val glomRDD: RDD[Array[Int]] = rdd.glom()
        val maxRDD: RDD[Int] = glomRDD.map(_.max)

        println(maxRDD.collect().sum)

        sc.stop()

    }

}
