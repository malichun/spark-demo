package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform2_groupby_test {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - groupBy
        val rdd = sc.textFile("datas/apache.log")

        val groupByRDD: RDD[(String, Iterable[String])] = rdd.groupBy(line => {
            line.split(":")(1)
        })

        groupByRDD.map{case (key, iter) => (key,iter.size)}.collect().foreach(println)
        sc.stop()

    }

}
