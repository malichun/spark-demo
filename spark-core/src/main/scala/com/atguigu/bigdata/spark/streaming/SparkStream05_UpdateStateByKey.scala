package com.atguigu.bigdata.spark.streaming

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 自定义数据采集器
 */
object SparkStream05_UpdateStateByKey {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streamWordCount")
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        ssc.checkpoint(".ck")

        val datas=ssc.socketTextStream("hadoop102",9999)

        val wordToOne = datas.map((_,1))
        // 无状态数据操作, 只对当前的采集周期内的数据进行处理
        // 在某些场合下 需要保留数据统计结果(状态), 实现数据的汇总
//        wordToOne
//            .reduceByKey(_+_)
//            .print()

        // updateStateByKey 根据key对数据的状态进行更新
        // 传递的参数中含有两个值
        // 第一个值: 表示相同key的value数据
        // 第二个值: 表示缓冲区的相同key的数据
        // 使用有状态操作需要设置checkpoint路径
        val state = wordToOne
            .updateStateByKey(
                (seq: Seq[Int], buffer: Option[Int]) => {
                    val newCount = buffer.getOrElse(0) + seq.sum
                    Option(newCount)
                }
            )

        state.print()

        ssc.start()
        ssc.awaitTermination()

    }

}
