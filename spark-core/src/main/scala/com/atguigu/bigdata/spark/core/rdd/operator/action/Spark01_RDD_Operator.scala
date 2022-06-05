package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4))

        // TODO - 行动算子
        // 所谓的行动算子其实就是触发作业执行的方法
        // 底层代码调用的是环境的runJob方法
        // 底层代码中会创建ActiveJob, 并提交job
        rdd.collect()

        sc.stop()

    }
}
