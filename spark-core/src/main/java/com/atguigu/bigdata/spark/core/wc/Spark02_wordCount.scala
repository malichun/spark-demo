package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_wordCount {
    def main(args: Array[String]): Unit = {

        // Application
        // Spark框架
        // TODO 简历和Spark框架的连接
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)

        // TODO 执行业务操作

        // 1.读取文件, 获取一行一行的数据
        // hello world
        val lines: RDD[String] = sc.textFile("datas")

        // 2. 将一行数据进行拆分,形成一个一个单词,(分词效果)
        // "hello world" => hello, world
        val words: RDD[String] = lines.flatMap(line => line.split(" "))

        val wordToOne: RDD[(String, Int)] = words.map((_, 1))

        val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(_._1)

        val wordToCount = wordGroup.map{
            case (word, list) =>
                list.reduce((t1,t2) => (t1._1, t1._2 +t2._2))
        }


         // 5. 将转换结果采集到控制台打印出来
        println(wordToCount.collect().mkString("Array(", ", ", ")"))
        // TODO 关闭连接
        sc.stop()

    }
}
