package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

// 自定义累加器
object Spark04_Acc_WordCount {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List("hello","spark","hello"))

        // rdd.map((_,1)).reduceByKey(_+_) // 有shuffle操作

        // 累加器: 实现wordCount
        // 创建累加器对象
        val wcAcc = new MyAccumulator()
        // 想Spark进行注册
        sc.register(wcAcc, "wcAcc")


        rdd.foreach(word => {
            // 数据的累加
            wcAcc.add(word)
        })

        // 获取累加器累加的结果
        println(wcAcc.value)


        sc.stop()
    }

    /**
     * 自定义数据累加器: WordCount
     *
     * 1. 继承 AccumulatorV2[IN, OUT]
     *     IN : 累加器输入的数据类型 String
     *     OUT : 累加器返回的数据类型 mutable.Map[String,Long]
     * 2. 重写方法
     *
     */
    class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]]{

        private val wcMap = mutable.Map[String,Long]()

        // 判断是否为初始状态
        override def isZero: Boolean = {
            wcMap.isEmpty
        }

        //
        override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
           new MyAccumulator()
        }

        // 重置
        override def reset(): Unit = wcMap.clear()

        // 获取累加器需要计算的值
        override def add(word: String): Unit =   {
            val newCnt = wcMap.getOrElse(word,0L) + 1
            wcMap.update(word, newCnt)
        }

        // Driver合并累加器
        override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
            val map1 = this.wcMap
            val map2 = other.value

            map2.foreach{case (word,count) =>
                val newCnt = map1.getOrElse(word, 0L) + count
                map1.update(word, newCnt)
            }
        }

        // 累加器结果
        override def value: mutable.Map[String, Long] = wcMap
    }

}
