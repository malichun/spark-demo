package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.SortedSet
import scala.collection.mutable

object Spark24_RDD_Req {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 案例实操
        val rdd = sc.textFile("datas/agent.log")

        rdd.map(line => {
            val arr = line.split(" ")
            val province = arr(1)
            val ad = arr(4)
            ((province,ad),1)
        })
            .reduceByKey(_+_)
            .map(t => (t._1._1,(t._1._2,t._2)))
//            .combineByKey(
//                (v:(String,Int)) => mutable.SortedSet(v)(Ordering.by((x:(String,Int)) => x._2).reverse),
//                (u:mutable.SortedSet[(String,Int)], v:(String,Int)) => {
//                    u+= v
//                    var uu: mutable.SortedSet[(String, Int)] = u;
//                    if(u.size > 3){
//                        uu = u.take(3)
//                    }
//                    uu
//                },
//                (u1:mutable.SortedSet[(String,Int)], u2:mutable.SortedSet[(String,Int)]) => {
//                    val u: mutable.SortedSet[(String, Int)] = u1.union( u2)
//                    var uu = u;
//                    if(u.size > 3){
//                        uu =u.take(3)
//                    }
//                    uu
//                }
//            ).collect().foreach(println)
            .groupByKey()
            .mapValues(iter => iter.toList.sortBy(_._2).reverse.take(3))
            .collect().foreach(println)

        sc.stop()
    }

}
