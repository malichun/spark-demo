package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File_Par {
    def main(args: Array[String]): Unit = {

        // TODO 准备环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 创建RDD
        // textFile可以将文件作为数据处理的数据源, 默认也可以设置分区
        //      minPartitions: 最小分区数量
        //      math.min(defaultParallelism, 2)
//        val rdd: RDD[String] = sc.textFile("datas/1.txt")

        // 如果不想使用默认分区数量, 可以通过第三个参数指定分区数
        // Spark读取文件底层采用Hadoop的读取方式
        // 分区数量的计算方式 getSplits
        // totalSize
        // goalSize = 7 / 2 = 3 (每个分区存放3个字节)

        // 7 / 3 = 2...1  有个1.1倍, 1.1+1 =3 (分区)
        val rdd: RDD[String] = sc.textFile("datas/1.txt", 2)
        rdd.saveAsTextFile("output")

        // TODO 关闭环境
        sc.stop()

    }

}
