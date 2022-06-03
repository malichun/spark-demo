package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark15_RDD_Operator_Transform_groupByKey {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - reduceByKey
        val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",4)))

        // groupByKey : 将数据源中的数据,相同key的数据分在同一个组中,形成一个对偶元祖
        //              元组中第一个元素就是key,
        //              元组中第二个元素就是相同可以的value的集合
        val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
        groupRDD.collect().foreach(println)

        val value: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)
        sc.stop()

    }

}
