package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStream06_State_Window1 {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streamWordCount")
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        ssc.checkpoint(".ck")

        val lines=ssc.socketTextStream("hadoop102",9999)
        val wordToOne = lines.map((_, 1))

        // reduceByKeyAndWindow: 当窗口范围比较大, 但是滑动幅度比较小, 那么可以采用增加数据和删除数据的方式
        // 无需重复计算
        val wordToCount = wordToOne.reduceByKeyAndWindow(
            (x,y) => (x+y),
            (x,y) => (x - y),
            Seconds(9),
            Seconds(3))



        wordToCount.print


        ssc.start()
        ssc.awaitTermination()

    }

}
