package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Transform_mappatitionswithindex {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - mapPartitionsWithIndex
        val rdd = sc.makeRDD(List(1,2,3,4), 2)

        // [1,2] [3,4]
        val mapPartitionsWithIndexRDD: RDD[Int] = rdd.mapPartitionsWithIndex((index,iter) => {
            if(index == 1){
                iter
            }else {
                Iterator.empty
            }
        })

        mapPartitionsWithIndexRDD.collect().foreach(println(_))

        sc.stop()

    }

}
