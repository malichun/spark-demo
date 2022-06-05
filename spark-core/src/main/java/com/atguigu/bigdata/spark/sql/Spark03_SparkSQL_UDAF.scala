package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StringType, StructField, StructType}

object Spark03_SparkSQL_UDAF {
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

        spark.udf.register("ageAvg", new MyAvgUDAF())

        spark.sql("select ageAvg(age) from user").show


        // TODO关闭环境
        spark.close()

    }


    /**
     * 自定义聚合函数
     * 1. 继承 UserDefinedAggregateFunction
     * 2. 实现方法
     */
    class MyAvgUDAF extends UserDefinedAggregateFunction {
        // 输入数据的结构
        override def inputSchema: StructType = StructType(StructField("age", LongType) :: Nil)

        // 缓冲区数据的结构
        override def bufferSchema: StructType = StructType(
            StructField("total", LongType) ::
            StructField("count", LongType) ::
            Nil)

        // 函数计算结构的数据类型
        override def dataType: DataType = LongType

        // 函数的稳定性
        override def deterministic: Boolean = true

        // 缓冲区初始化
        override def initialize(buffer: MutableAggregationBuffer): Unit = {
//            buffer(0) = 0L
//            buffer(1) = 10L

            buffer.update(0,0L)
            buffer.update(1, 0L)
        }

        // 根据输入的值更新缓冲区
        override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

            buffer.update(0, buffer.getLong(0) + input.getLong(0))
            buffer.update(1, buffer.getLong(1) +1L)
        }

        // 缓冲区数据合并
        override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
            buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
            buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
        }

        override def evaluate(buffer: Row): Any = {
            (buffer.getLong(0) / buffer.getLong(1))
        }
    }

}
