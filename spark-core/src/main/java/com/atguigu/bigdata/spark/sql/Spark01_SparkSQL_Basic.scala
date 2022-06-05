package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark01_SparkSQL_Basic {
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
        // df.show()

        // DataFrame => SQL
        df.createOrReplaceTempView("user")
        spark.sql(
            s"""
               |select * from user
               |""".stripMargin)
            .show

        spark.sql("select avg(age) from user").show

        // DataFrame => DSL
        // 在使用DataFrame时, 如果涉及到转换操作, 需要转换规则
        df.select('age + 1).show


        // DataSet
        // DataFrame是特定泛型的DataSet
        val seq = Seq(1,2,3,4)
        val ds = seq.toDS()
        ds.show()


        // RDD <=> DataFrame
        val rdd = spark.sparkContext.makeRDD(List((1,"zhangsan",30),(2,"lisi",40)))
        val df1 = rdd.toDF("id", "name", "age")
        val rdd1 = df1.rdd

        // DataFrame <=> DataSet
        val ds1 = df1.as[User]
        val df2 = ds1.toDF()

        // RDD <=> DataSet
        val ds3 = rdd.map(t => User(t._1, t._2, t._3)).toDS()
        val rdd2 = ds3.rdd



        // TODO关闭环境
        spark.close()

    }

    case class User(id:Int, name:String, age:Int)
}
