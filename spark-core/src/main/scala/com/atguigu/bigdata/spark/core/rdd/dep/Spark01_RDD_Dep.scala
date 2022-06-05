package com.atguigu.bigdata.spark.core.rdd.dep

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark01_RDD_Dep {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)
        val lines: RDD[String] = sc.textFile("datas/word.txt")

        println(lines.toDebugString) // 血缘关系
        println("*******************************")
        val words: RDD[String] = lines.flatMap(line => line.split(" "))
        println(words.toDebugString) // 血缘关系
        println("*******************************")
        val wordToOne = words.map(word => (word,1))
        println(wordToOne.toDebugString) // 血缘关系
        println("*******************************")
        val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
        println(wordToSum.toDebugString) // 血缘关系
        println("*******************************")
        val array = wordToSum.collect()

        array.foreach(println)

        sc.stop()

    }
}
