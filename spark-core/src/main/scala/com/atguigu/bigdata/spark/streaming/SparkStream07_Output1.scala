package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStream07_Output1 {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streamWordCount")
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        ssc.checkpoint(".ck")

        val lines=ssc.socketTextStream("hadoop102",9999)
        val wordToOne = lines.map((_, 1))

        val windowDS = wordToOne.reduceByKeyAndWindow(
            (x,y) => (x+y),
            (x,y) => (x - y),
            Seconds(9),
            Seconds(3))


        // foreachRDD 不会出现时间戳
        windowDS.foreachRDD( rdd => {

        })



        ssc.start()
        ssc.awaitTermination()

    }

}
