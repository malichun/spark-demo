package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark18_RDD_Operator_Transform3 {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - reduceByKey
        val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("a",4)),2)

        // aggregateByKey最终的返回数据结果应该和初始值的类型保持一致

        // 获取相同key的数据的平均值
        rdd.aggregateByKey((0,0))((u,v) => (u._1+v,u._2+1),
            (u1,u2) => (u1._1+u2._1,u1._2+u2._2)
        ).mapValues{case (sum,cnt) => sum / cnt}.collect().foreach(println)
        sc.stop()

    }

}
