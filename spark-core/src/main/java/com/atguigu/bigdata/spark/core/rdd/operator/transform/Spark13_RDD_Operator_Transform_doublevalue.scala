package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_RDD_Operator_Transform_doublevalue {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - 双Value类型操作 intersection
        // 交集,并集和差集要求两个数据源数据类型保持一致
        // 拉链操作两个数据源操作类型可以不一致

        val rdd1 = sc.makeRDD(List(1,2,3,4,9,9))
        val rdd2 = sc.makeRDD(List(3,4,5,6,9,9))

        // 交集(只显示去重的) [9,3,4]
        val rdd3: RDD[Int] = rdd1.intersection(rdd2)
        println(rdd3.collect().mkString(","))

        // 并集(不会去重) [1,2,3,4,9,9,3,4,5,6,9,9]
        val rdd4: RDD[Int] = rdd1.union(rdd2)
        println(rdd4.collect().mkString(","))

        // 差集  [1,2]
        val rdd5: RDD[Int] = rdd1.subtract(rdd2)
        println(rdd5.collect().mkString(","))

        // 拉链 (1,3),(2,4),(3,5),(4,6),(9,9),(9,9)
        val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
        println(rdd6.collect().mkString(","))


        sc.stop()

    }

}
