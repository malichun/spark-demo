package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

/**
 * 恢复数据
 */
object SparkStream09_Resume {
    def main(args: Array[String]): Unit = {
        val ssc = StreamingContext.getActiveOrCreate("ck", () => {
            val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streamWordCount")
            val ssc = new StreamingContext(sparkConf, Seconds(3))
            val lines=ssc.socketTextStream("hadoop102",9999)

            val wordToOne = lines.map((_,1))


            wordToOne.print()


            ssc
        })

        ssc.checkpoint("ck")


        ssc.start()

        ssc.awaitTermination() // 阻塞main线程

    }

}
