package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStream06_State_Transform {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streamWordCount")
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        ssc.checkpoint(".ck")

        val datas=ssc.socketTextStream("hadoop102",9999)

        // transform方法可以将底层RDD获取到后进行操作
        // 1. DStream功能不完善
        // 2. 需要代码周期性的执行

        // Code: Driver
        val DStream = datas.transform(rdd => {
            // Code: Driver,(周期性执行,一个采集周期执行一次)
                rdd.map( str => {
                    // Code: Executor
                    str
                })
            }
        )

        // Code: Driver
        datas.map(data =>{
            // Code: Executor
            data
        })

        ssc.start()
        ssc.awaitTermination()

    }

}
