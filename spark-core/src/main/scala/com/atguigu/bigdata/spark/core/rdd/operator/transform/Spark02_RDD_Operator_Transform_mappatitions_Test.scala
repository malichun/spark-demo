package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Transform_mappatitions_Test {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - mapPartitions
        val rdd = sc.makeRDD(List(1,2,3,4), 2)
        // [1,2],[3,4]
        val mapPartitionsRDD = rdd.mapPartitions(iter => List(iter.max).iterator)

        mapPartitionsRDD.collect().foreach(println)
        sc.stop()

    }

}
