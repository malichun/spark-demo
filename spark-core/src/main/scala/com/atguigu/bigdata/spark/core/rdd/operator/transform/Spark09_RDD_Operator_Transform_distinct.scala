package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_RDD_Operator_Transform_distinct {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - distinct
        val rdd = sc.makeRDD(List(1,2,3,4,1,2,3,4))

        //map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
        val rdd1 = rdd.distinct()

        rdd1.collect().foreach(println)

        // 自己实现
        val rdd2: RDD[Int] = rdd1.map((_, null)).reduceByKey((x, _) => x).map(_._1)
        rdd2.collect().foreach(println)

        sc.stop()

    }

}
