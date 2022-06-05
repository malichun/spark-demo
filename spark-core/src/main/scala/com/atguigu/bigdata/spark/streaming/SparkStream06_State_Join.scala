package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStream06_State_Join {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streamWordCount")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        ssc.checkpoint(".ck")

        val data9999=ssc.socketTextStream("hadoop102",9999)
        val data8888=ssc.socketTextStream("hadoop102",8888)

        val map9999 = data9999.map((_, 9))
        val map8888 = data8888.map((_, 8))

        // 所谓的DStream的Join操作, 其实就是两个RDD的join
        val joinDS = map9999.join(map8888)

        joinDS.print()

        ssc.start()
        ssc.awaitTermination()

    }

}
