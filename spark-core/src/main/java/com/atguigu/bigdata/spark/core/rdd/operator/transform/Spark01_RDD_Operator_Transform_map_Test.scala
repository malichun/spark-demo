package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_map_Test {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - map
        val rdd: RDD[String] = sc.textFile("datas/apache.log")
        // 长的字符串
        // 短的字符串
        val mapRDD: RDD[String] = rdd.map(line => {
            val arr = line.split(" ")
            arr(6)
        })
        mapRDD.collect().foreach(println)
        sc.stop()

    }

}
