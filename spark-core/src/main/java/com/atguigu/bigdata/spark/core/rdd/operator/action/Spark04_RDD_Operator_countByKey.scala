package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_countByKey {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,1,1,4), 2)
        //Map( 1 -> 3, 4 -> 1)
        val intToLong: collection.Map[Int, Long] = rdd.countByValue()
        println(intToLong)


        val rdd2 = sc.makeRDD(List(
            ("a",1),("a",1),("a",1)
        ))

        val stringToLong: collection.Map[String, Long] = rdd2.countByKey()
        println(stringToLong)

        sc.stop()

    }
}
