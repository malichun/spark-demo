package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Operator_Transform_repartition {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - coalesce
        val rdd = sc.makeRDD(List(1,2,3,1,2,3,1,1,1,1,1),2)


        // coalesce可以扩大分区,前提走shuffle
        // spark提供了简化的操作
        // 缩减分区用coalesce,如果想让数据均衡,使用shuffle
//        val newRDD = rdd.coalesce(3, true)
        val newRDD = rdd.repartition(3)

        newRDD.saveAsTextFile("output")

        sc.stop()

    }

}
