package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Transform_glom {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - mapPartitionsWithIndex
        val rdd = sc.makeRDD(List(1,2,3,4), 2)

        // List => Int
        // Int => Array
        val glomRDD: RDD[Array[Int]] = rdd.glom()
        glomRDD.collect().foreach(data => println(data.mkString(",")))
        sc.stop()

    }

}
