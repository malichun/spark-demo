package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object SparkStream08_Close {
    def main(args: Array[String]): Unit = {

        /*
        线程的关闭
        val thread = new Thread()
        thread.start()

        thread.stop(); // 强制关闭


         */

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streamWordCount")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        val lines=ssc.socketTextStream("hadoop102",9999)

        val wordToOne = lines.map((_,1))


        wordToOne.print()
        ssc.start()

        // 如果想要关闭采集器, 那么需要创建新的线程
        // 而且需要在第三方的程序中增加关闭状态
        new Thread(() => {

            // 优雅的关闭
            // 计算节点不再接收新的数据, 而是将现有数据处理完毕,然后关闭
            // Mysql: Table(stopStart) => Row => data
            // Redis: Data(K-V)
            // ZK: /stopSpark
            // HDFS: /stopSpark
//            while(true){
//
//                if(true){
//                    // 获取SparkStreaming状态
//                    val state = ssc.getState()
//                    if(state == StreamingContextState.ACTIVE){
//                        ssc.stop(true, true)
//                    }
//                }
//                Thread.sleep(5000)
//            }

            // 测试
            Thread.sleep(5000)
            val state = ssc.getState()
            if(state == StreamingContextState.ACTIVE){
                ssc.stop(true, true)
            }
            System.exit(0)
        }).start()



        ssc.awaitTermination() // 阻塞main线程

    }

}
