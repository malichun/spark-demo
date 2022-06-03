package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform1_flatMap {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(
            "hello Scala", "Hello Spark"
        ))

        val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

        flatRDD.collect().foreach(println)

        sc.stop()

    }

}
