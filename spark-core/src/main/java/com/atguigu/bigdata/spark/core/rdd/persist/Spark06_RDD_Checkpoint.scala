package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Checkpoint {
  def main(args: Array[String]): Unit = {

    // cache: 将数据临时存储在内存中进行数据重用
    //        会在血缘关系中添加新的依赖. 一旦出现问题, 可以重新读取数据
    // persist: 将数据临时存储在磁盘文件中进行数据重用
    //          涉及到磁盘io, 性能较低, 但是数据安全
    //          如果作业执行完毕, 临时保存的数据文件就会丢失
    // checkpoint: 将数据长久保存在磁盘文件中进行数据重用
    //          涉及到磁盘io, 性能较低, 但是数据安全
    //          为了保证数据安全, 所以一般情况下,会独立执行作业(checkpoint单独执行一次)
    //          为了能够提高效率,一般情况下, 是需要和cache联合使用的
    //          执行过程中会切断血缘关系. 重新建立新的血缘关系
    //          checkpoint等同于改变数据源

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("./checkpoint")

    val list = List("Hello Scala", "Hello Spark")

    val rdd = sc.makeRDD(list)

    val flatRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatRDD.map(word => {
      println("@@@@@@@@@@@@@@@@@@@@@@@@@@")
      (word, 1)
    })
//    mapRDD.cache()
    mapRDD.checkpoint()

    println(mapRDD.toDebugString)
    val reduceRDD = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    println("*******************************************************")
    println(mapRDD.toDebugString)


    sc.stop()

  }

}
