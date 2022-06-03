package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform2_flatMap {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(List(1,2), 3, List(4,5)))
        val flatRDD = rdd.flatMap {
            case list: List[_] => list
            case dat => List(dat)
        }
        flatRDD.collect().foreach(println)
        sc.stop()

    }

}
