package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

// 自定义累加器
object Spark05_Broad {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
        val sc = new SparkContext(sparkConf)

        val rdd1 = sc.makeRDD(List(
            ("a",1),("b",2),("c",3)
        ))

//        val rdd2 = sc.makeRDD(List(
//            ("a",4),("b",5),("c",6)
//        ))

        val map = mutable.Map(("a",4),("b",5),("c",6))

        // join会导致数据量几何增长, 会影响shuffle的性能, 不推荐使用
//        val joinRDD = rdd1.join(rdd2)
        // (a,(1,4)), (b,(2,5)), (c,(3,6))

        rdd1.map{
            case (w, c) => {
                val l = map.getOrElse(w, 0)
                (w,(c, l))
            }
        }.collect.foreach(println)


        sc.stop()
    }

}
