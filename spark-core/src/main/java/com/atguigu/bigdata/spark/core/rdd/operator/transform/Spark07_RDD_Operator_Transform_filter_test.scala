package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_Transform_filter_test {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.textFile("datas/apache.log")

        val filterRDD = rdd.filter(line => {
            line contains "17/05/2015"
        })

        filterRDD.collect().foreach(println)
        sc.stop()

    }

}
