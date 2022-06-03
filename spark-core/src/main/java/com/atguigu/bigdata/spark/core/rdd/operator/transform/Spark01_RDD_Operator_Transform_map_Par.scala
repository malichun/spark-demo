package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_map_Par {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - map
        // 1. rdd的计算 一个分区内的数据是一个一个执行逻辑
        //    只有前面一个数全部的逻辑执行完毕后,才会执行下一个数据
        //    分区内数据的执行时有序的
        // 2. 不同分区数据计算是无序的
        val rdd = sc.makeRDD(List(1,2,3,4),2)
        val mapRDD = rdd.map(num => {
            println(">>>>>>"+ num)
            num
        })

        val mapRDD1 = mapRDD.map(num => {
            println("######"+ num)
            num
        })

        mapRDD1.collect()
        sc.stop()

    }

}

// 并行度调整为1结果
//>>>>>>1
//######1
//>>>>>>2
//######2
//>>>>>>3
//######3
//>>>>>>4
//######4

// 并行度为2 的结果
//>>>>>>3
//######3
//>>>>>>4
//######4
//>>>>>>1
//######1
//>>>>>>2
//######2
