package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark19_RDD_Operator_Transform3_combineByKey {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - reduceByKey
        val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("a",4)),2)

        // combineByKey: 方法需要三个参数
        // 第一个参数: 将相同key的第一个数据进行结构转换, 实现操作
        // 第二个参数: 分区内的计算规则
        // 第三个参数: 分区间的计算规则
        rdd.combineByKey(v=> (v,1),
            (t:(Int,Int), v) => (t._1+v, t._2+1),
            (t1:(Int,Int), t2:(Int,Int)) => (t1._1+t2._1, t1._2+t2._2)
        ).mapValues(t => t._1 / t._2).collect().foreach(println)
    }

}
