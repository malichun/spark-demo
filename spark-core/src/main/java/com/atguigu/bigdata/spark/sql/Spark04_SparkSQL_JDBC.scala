package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}

object Spark04_SparkSQL_JDBC {
    def main(args: Array[String]): Unit = {
        // TODO 创建SparkSQL的运行环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")

        val spark = SparkSession.builder()
            .config(sparkConf)
            .getOrCreate()
        import spark.implicits._
        // TODO 执行逻辑操作
        val df = spark.read.format("jdbc")
            .option("url", "jdbc:mysql://hadoop102:3306/sparksql")
            .option("driver", "com.mysql.jdbc.Driver")
            .option("user", "root")
            .option("password", "123456")
            .option("dbtable", "user")
            .load()

        df.show

        // 保存数据
        df.write.format("jdbc")
            .option("url", "jdbc:mysql://hadoop102:3306/sparksql")
            .option("driver", "com.mysql.jdbc.Driver")
            .option("user", "root")
            .option("password", "123456")
            .option("dbtable", "user1")
            .mode(SaveMode.Append)
            .save()

        // TODO 关闭环境
        spark.close()

    }



}
