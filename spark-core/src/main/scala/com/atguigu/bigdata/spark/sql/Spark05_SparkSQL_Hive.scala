package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object Spark05_SparkSQL_Hive {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME","atguigu")
        // TODO 创建SparkSQL的运行环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")

        val spark = SparkSession.builder()
            .enableHiveSupport()
            .config("spark.sql.warehouse.dir", "hdfs://hadoop102:9820/user/hive/warehouse")
            .config(sparkConf)
            .getOrCreate()

        // 使用SparkSql连接外置的Hive
        // 1.拷贝hive-site.xml到classpath下
        // 2. 启动hive的支持
        // 3. 增加对应的依赖关系(包含mysql的驱动)
        spark.sql("show tables").show(false)


        // TODO 关闭环境
        spark.close()

    }



}
