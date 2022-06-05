package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

//在需求一的基础上，增加每个品类用户session的点击统计
object Spark02_Req1_HotCategoryTop10SessionAnalysis {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
        val sc = new SparkContext(sparkConf)
        val actionRDD = sc.textFile("datas/user_visit_action.txt")

        actionRDD.cache()
        val top10Ids = top10Category(actionRDD).toSet

        val broadCast = sc.broadcast(top10Ids)


        // 1. 过滤原始数据, 保留点击和前10 的品类的id的数据
        actionRDD.mapPartitions( iter => {
            iter.map[((String, String),Int)](action => {
                val arr = action.split("_")
                val setIds = broadCast.value
                if(setIds.contains(arr(6))){
                    ((arr(6), arr(2)),1)
                }else{
                    null
                }
            }).filter(_!=null)
        })
            .reduceByKey(_+_)
            .mapPartitions(iter => {
                implicit val cntOrdering:Ordering[(String,Int)] = new Ordering[(String,Int)]{
                    override def compare(x: (String, Int), y: (String, Int)): Int = y._2.compareTo(x._2)
                }
                iter.foldLeft(
                    mutable.SortedMap[String, mutable.SortedSet[(String,Int)]]()
                )((m,e) => {
                    val categoryId = e._1._1
                    val sessionId = e._1._2
                    val cnt = e._2
                    val sortedSet = m.getOrElse(categoryId,mutable.SortedSet[(String,Int)]())
                    sortedSet.add((sessionId,cnt))
                    if(sortedSet.size > 10){
                        sortedSet.drop(1)
                    }
                    m.update(categoryId,sortedSet)
                    m
                }).flatMap(t => {
                    val value = t._2
                    value.map( e => (t._1,(e._1,e._2)))
                }).iterator
            })
            .groupByKey()
            .map{case (category, iter) => (category, iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10))}
            .collect()
            .foreach(println)


        sc.stop()
    }

    def top10Category(actionRDD:RDD[String]) = {
        // (品类ID, (点击数量, 下单数量, 支付数量))
        actionRDD.flatMap{ action =>
            val datas = action.split("_")
            if(datas(6) != "-1"){
                List((datas(6), (1,0,0)))
            }else if(datas(8) != "null"){
                datas(8).split(",").map(id => (id, (0,1,0)))
            }else if(datas(10) != "null"){
                datas(10).split(",").map(id => (id, (0,0,1)))
            }else{
                Nil
            }
        }
            .reduceByKey((t1,t2) => (t1._1+t2._1, t1._2+t2._2, t1._3+t2._3))
            .sortBy(_._2,false)
            .take(10)
            .map(_._1)
    }

}
