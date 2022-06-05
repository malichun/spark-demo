package com.atguigu.bigdata.spark.core.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Dep {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)
        val lines: RDD[String] = sc.textFile("datas/word.txt")

        println(lines.dependencies) // 血缘关系
        println("*******************************")
        val words: RDD[String] = lines.flatMap(line => line.split(" "))
        println(words.dependencies) // 血缘关系
        println("*******************************")
        val wordToOne = words.map(word => (word,1))
        println(wordToOne.dependencies) // 血缘关系
        println("*******************************")
        val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
        println(wordToSum.dependencies) // 血缘关系
        println("*******************************")
        val array = wordToSum.collect()

        array.foreach(println)

        sc.stop()

    }
}
