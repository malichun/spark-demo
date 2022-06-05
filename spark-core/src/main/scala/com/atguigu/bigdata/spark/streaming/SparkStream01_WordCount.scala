package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}


object SparkStream01_WordCount {
    def main(args: Array[String]): Unit = {

        // TODO 创建环境对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streamWordCount")
        // StreamingContext 创建时, 需要传递两个参数
        // 第一个参数表示环境配置
        // 第二个参数表示批量处理的周期(采集周期)
        val ssc = new StreamingContext(sparkConf, Seconds(3))


        // TODO 逻辑处理
        // 获取端口数据
        val lines = ssc.socketTextStream("hadoop102", 9999)

        val words = lines.flatMap(_.split(" "))

        val wordToOne = words.map((_, 1))

        val wordToCount = wordToOne.reduceByKey(_ + _)

        wordToCount.print()

       //由于sparkStreaming采集器是长期执行的任务, 所有不能直接关闭
        // 如果main方法执行完毕, 应用程序也会自动结束, 所以不能让main方法执行完毕
        //ssc.stop()
        // 1. 启动采集器
        ssc.start()
        // 2. 等待采集器的执行
        ssc.awaitTermination()

    }
}
