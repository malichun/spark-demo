package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_RDD_Operator_Transform3_join {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - (Key - Value类型) join
        val rdd1 = sc.makeRDD(List(("a",1),("b",2),("c",3),("a",1),("b",2),("c",3)))
        val rdd2 = sc.makeRDD(List(("a",4),("b",5),("c",6)))

        // join: 两个不同数据源的数据, 相同的key的value会连接在一起,形成元组
        //       如果两个数据源中,key没有匹配上,那么数据不会出现在结果中
        //       如果两个数据源中key有多个相同的, 则会有n*m个key的数据,数据量会几何性增长
        val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

        joinRDD.collect().foreach(println)

        sc.stop()
    }

}
