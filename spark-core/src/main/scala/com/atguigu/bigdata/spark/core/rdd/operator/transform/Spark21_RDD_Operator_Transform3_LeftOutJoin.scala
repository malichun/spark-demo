package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_RDD_Operator_Transform3_LeftOutJoin {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - (Key - Value类型) join
        val rdd1 = sc.makeRDD(List(("d",1),("a",1),("b",2),("c",3),("a",1),("b",2),("c",3)))
        val rdd2 = sc.makeRDD(List(("a",4),("b",5),("c",6),("f",6)))

        val leftJoinRDD: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)
        val rightJoinRDD: RDD[(String, (Option[Int], Int))] = rdd1.rightOuterJoin(rdd2)
        leftJoinRDD.collect().foreach(println(_))
        println("---------------")
        rightJoinRDD.collect().foreach(println(_))
        sc.stop()
    }

}
