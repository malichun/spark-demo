package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform_flatMap {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - mapPartitionsWithIndex
        val flatRdd = sc.makeRDD(List(
            List(1,2), List(3,4)
        )).flatMap(x => x)

        flatRdd.collect().foreach(println)

        sc.stop()

    }

}
