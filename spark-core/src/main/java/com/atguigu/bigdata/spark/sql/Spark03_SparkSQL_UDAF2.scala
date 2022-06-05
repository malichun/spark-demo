package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

object Spark03_SparkSQL_UDAF2 {
    def main(args: Array[String]): Unit = {
        // TODO 创建SparkSQL的运行环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")

        val spark = SparkSession.builder()
            .config(sparkConf)
            .getOrCreate()
        import spark.implicits._
        // TODO 执行逻辑操作


        //RDD

        // DataFrame
        val df = spark.read.json("datas/user.json")

        // 早期版本中, spark不能在sql使用强类型udaf操作
        // SQL & DSL
        // 早期的UDAF强类型聚合函数使用DSL语法操作
        val ds = df.as[User]
        // 将UDAF转换为查询的列对象
        val udafCol = new MyAvgUDAF().toColumn

        ds.select(udafCol)
            .show



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
    case class User(username:String, age:Long)
    case class Buff(var total:Long, var count:Long)
    class MyAvgUDAF extends Aggregator[User, Buff, Long]{
        // 零值, 初始值
        override def zero: Buff = Buff(0,0)

        // 根据输入的数据更新缓冲区的数据
        override def reduce(b: Buff, a: User): Buff = {
            b.total = b.total + a.age
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
