package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 查看数据落到哪个分区文件
 */
object Spark01_RDD_Memory_Par1 {
    def main(args: Array[String]): Unit = {

        // TODO 准备环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 创建RDD

        // [1,2], [3,4]
//        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)

        // [1], [2], [3,4]
//        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),3)

        // [1], [2,3], [4,5]
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5),3)


        // def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
        //      (0 until numSlices).iterator.map { i =>
        //        val start = ((i * length) / numSlices).toInt
        //        val end = (((i + 1) * length) / numSlices).toInt
        //        (start, end)
        //      }
        //    }
        // 然后slice

        // 将处理的数据保存成分区文件
        rdd.saveAsTextFile("output")

        // TODO 关闭环境
        sc.stop()


    }

}
