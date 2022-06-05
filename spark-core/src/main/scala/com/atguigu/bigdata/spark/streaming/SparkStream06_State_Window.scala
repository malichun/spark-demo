package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStream06_State_Window {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streamWordCount")
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        ssc.checkpoint(".ck")

        val lines=ssc.socketTextStream("hadoop102",9999)
        val wordToOne = lines.map((_, 1))

        //窗口的范围应该是采集的周期整数倍
        // 窗口可以滑动的, 但是默认情况下, 一个采集周期进行滑动
        // 这样的话可能会出现重复数据的计算, 为了避免这种情况, 我们可以改变滑动的幅度(步长)
        val windowDS = wordToOne.window(Seconds(6), Seconds(6)) // 就变成了滚动窗口

        val wordToCount = windowDS.reduceByKey(_ + _)

        wordToCount.print


        ssc.start()
        ssc.awaitTermination()

    }

}
