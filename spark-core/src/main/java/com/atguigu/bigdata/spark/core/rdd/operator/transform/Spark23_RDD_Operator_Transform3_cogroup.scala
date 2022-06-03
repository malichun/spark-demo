package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark23_RDD_Operator_Transform3_cogroup {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - (Key - Value类型) cogroup
        val rdd1 = sc.makeRDD(List(("d",1),("a",1),("b",2),("c",3),("a",1),("b",2),("c",3)))
        val rdd2 = sc.makeRDD(List(("a",4),("b",5),("c",6),("f",6)))

        // 分组连接
        val cgRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
        cgRDD.collect().foreach(println)


        sc.stop()
    }

}
