package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.Random

/**
 * 自定义数据采集器
 */
object SparkStream03_DIY {
    def main(args: Array[String]): Unit = {

        // TODO 创建环境对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streamWordCount")
        // StreamingContext 创建时, 需要传递两个参数
        // 第一个参数表示环境配置
        // 第二个参数表示批量处理的周期(采集周期)
        val ssc = new StreamingContext(sparkConf, Seconds(3))


        val message = ssc.receiverStream(new MyReceiver)
        message.print()

        ssc.start()
        ssc.awaitTermination()

    }

    /**
     * 自定义数据采集器
     * 1. 集成Receiver,定义泛型, 选取参数
     * 2. 实现方法
     */
    class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY){
        @volatile
        private var flag:Boolean = true

        override def onStart(): Unit = {

            new Thread(() => {
                while (flag) {
                    val message = "采集的数据为" + new Random().nextInt(10).toString
                    // 生成数据
                    store(message)
                    Thread.sleep(500)
                }
            }).start()
        }

        override def onStop(): Unit = {
            flag = false
        }
    }
}
