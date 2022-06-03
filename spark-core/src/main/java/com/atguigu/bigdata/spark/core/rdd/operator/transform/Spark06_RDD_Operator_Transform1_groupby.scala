package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform1_groupby {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - groupBy
        val rdd = sc.makeRDD(List("Hello","Spark","Scala","Hadoop"))

        // 分组的分区没有必然的关系
        // groupBy会将数据源中的每一个数据进行分组判断,根据返回的分组key进行分组
        val groupRDD: RDD[(Char, Iterable[String])] = rdd.groupBy(_.charAt(0))
        groupRDD.collect().foreach(println)

        sc.stop()

    }

}
