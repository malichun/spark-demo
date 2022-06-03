package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_Operator_Transform_coalesce {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - coalesce
        val dataRDD = sc.makeRDD(List(
            1,2,3,4,1,2
        ),6)

        // coalesce方法默认不会将数据打散重新组合
        // 这种情况下的缩减分区可能会导致数据不均衡, 出现数据倾斜
        // 如果想要让数据均衡, 可以进行shuffle处理
//        val dataRDD1 = dataRDD.coalesce(2)
        val dataRDD1 = dataRDD.coalesce(2, true)

        dataRDD1.glom().collect

        sc.stop()

    }

}
