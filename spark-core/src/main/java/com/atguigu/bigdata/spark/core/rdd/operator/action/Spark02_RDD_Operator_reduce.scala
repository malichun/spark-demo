package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_reduce {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4))

        // TODO - 行动算子
        // reduce
        val i: Int = rdd.reduce(_ + _)
        println(i)

        // collect, 方法会将不同分区的数据按照分区顺序采集到的Driver端内存中,形成数组
        val ints: Array[Int] = rdd.collect()
        println(ints.mkString(","))

        // count: rdd数据源中数据的个数
        val count: Long = rdd.count()
        println(count)

        // first: 获取数据源中数据的第一个
        val first: Int = rdd.first()
        println(first)

        // take: 获取n个数据
        val ints1: Array[Int] = rdd.take(3)
        println(ints1.mkString(","))

        // takeOrdered: 数据排序后, 取N个数据
        val rdd1 = sc.makeRDD(List(4,2,3,1))
        val ints2: Array[Int] = rdd1.takeOrdered(3)
        println(ints2.mkString(","))

        sc.stop()

    }
}
