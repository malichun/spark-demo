package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat

object Spark02_RDD_Persist {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val list = List("Hello Scala", "Hello Spark")

    val rdd = sc.makeRDD(list)

    val flatRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatRDD.map(word => {
      println("@@@@@@@@@@@@@@@@@@@@@@@@@@")
      (word, 1)
    }
    )

    val reduceRDD = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    println("*******************************************************")


    val groupRDD = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)

    sc.stop()

  }

}
