package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession, functions}

object Spark03_SparkSQL_UDAF1 {
    def main(args: Array[String]): Unit = {
        // TODO 创建SparkSQL的运行环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")

        val spark = SparkSession.builder()
            .config(sparkConf)
            .getOrCreate()
        // TODO 执行逻辑操作


        //RDD

        // DataFrame
        val df = spark.read.json("datas/user.json")
        df.createOrReplaceTempView("user")
        spark.sql("select * from user").show

        spark.udf.register("ageAvg", functions.udaf(new MyAvgUDAF))

        spark.sql("select ageAvg(age) from user").show


        // TODO关闭环境
        spark.close()

    }


    /**
     * 自定义聚合函数
     * 1. 继承 org.apache.spark.sql.expressions.Aggregator, 定义泛型
     *  IN :  输入的数据类型 (Long)
     *  BUF:  缓冲的数据类型 Buff(Long,Long)
     *  OUT:  输出的数据类型 Long
     * 2. 实现方法
     */
    case class Buff(var total:Long, var count:Long)
    class MyAvgUDAF extends Aggregator[Long, Buff, Long]{
        // 零值, 初始值
        override def zero: Buff = Buff(0,0)

        // 根据输入的数据更新缓冲区的数据
        override def reduce(b: Buff, a: Long): Buff = {
            b.total = b.total + a
            b.count = b.count + 1
            b
        }

        // 合并缓冲区
        override def merge(b1: Buff, b2: Buff): Buff = {
            b1.total = b1.total + b2.total
            b1.count = b1.count + b2.count
            b1
        }

        // 计算结果
        override def finish(reduction: Buff): Long = {
            reduction.total / reduction.count
        }

        // 缓冲区的编码操作, 自定义类
        override def bufferEncoder: Encoder[Buff] = Encoders.product

        // 输出的编码操作 , scala存在的类
        override def outputEncoder: Encoder[Long] = Encoders.scalaLong
    }

}
