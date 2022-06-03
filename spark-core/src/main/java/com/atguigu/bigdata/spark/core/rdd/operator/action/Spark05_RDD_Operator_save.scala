package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_save {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(
            ("a",1),("a",1),("a",1)
        ))

        rdd.saveAsTextFile("output")
        rdd.saveAsObjectFile("output1")
        // saveAsSequenceFile要求必须是K-V类型
        rdd.saveAsSequenceFile("output2")


        sc.stop()

    }
}
