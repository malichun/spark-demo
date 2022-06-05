package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Transform_mappatitions {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - mapPartitions
        val rdd = sc.makeRDD(List(1,2,3,4), 2)

        // 有多少分区执行多少次
        // mapPartitions: 可以以分区为单位进行数据转换操作
        //                但是会将整个分区的数据加载到内存中引用
        //                如果处理完的数据是不会被释放掉,存在对象的引用
        //                在内存较小,数据量较大场合下,容易出现内存溢出
        val mapPartitionsRDD: RDD[Int] = rdd.mapPartitions(iter => {
            println(">>>>>>>>>")
            iter.map(_ * 2)
        })

        mapPartitionsRDD.collect().foreach(println(_))

        sc.stop()

    }

}
