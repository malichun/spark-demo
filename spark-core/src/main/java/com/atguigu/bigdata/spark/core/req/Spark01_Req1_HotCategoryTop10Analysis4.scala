package com.atguigu.bigdata.spark.core.req

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

// 使用累加器, 不走shuffle
object Spark01_Req1_HotCategoryTop10Analysis4 {
    def main(args: Array[String]): Unit = {
        // TODO: Top10热门品类

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
        val sc = new SparkContext(sparkConf)

        // 1. 读取原始的日志数据
        val actionRDD = sc.textFile("datas/user_visit_action.txt")

        val acc = new HotCategoryAccumulator()
        sc.register(acc, "hotCategory")
        // 2.将数据结构转换
        actionRDD.foreach(action => {
            val datas = action.split("_")
            if(datas(6) != "-1"){
                acc.add((datas(6),"click"))
            }else if(datas(8) != "null"){
                datas(8).split(",").foreach(id => acc.add(id,"order"))
            }else if(datas(10) != "null"){
                datas(10).split(",").foreach(id => acc.add(id,"pay"))
            }
        })

        val map = acc.value
        map.values.toList.sorted.take(10).foreach(println)

        sc.stop()

    }

    case class HotCategory(cid:String,var clickCnt:Int = 0,var orderCnt:Int = 0,var payCnt:Int = 0) extends Comparable[HotCategory]{
        override def compareTo(o: HotCategory): Int = {
            val r1 = o.clickCnt.compareTo(this.clickCnt)
            if(r1 != 0){
                return r1
            }
            val r2 = o.orderCnt.compareTo(this.orderCnt)
            if(r2 !=0){
                return r2
            }
            o.payCnt.compareTo(this.payCnt)
        }
    }
    /**
     * 自定义累加器
     * IN : (品类ID, 行为类型)
     * OUT : mutable.Map[String,HotCategory]
     */
    class HotCategoryAccumulator extends AccumulatorV2[(String,String), mutable.Map[String, HotCategory]]{

        private val hcMap = mutable.Map[String, HotCategory]()

        override def isZero: Boolean = hcMap.isEmpty

        override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = new HotCategoryAccumulator

        override def reset(): Unit = hcMap.clear()

        override def add(v: (String, String)): Unit = {
            val cid = v._1
            val actionType = v._2
            val category = hcMap.getOrElse(cid, HotCategory(cid))
            if(actionType == "click"){
                category.clickCnt += 1
            }else if(actionType == "order"){
                category.orderCnt += 1
            }else if(actionType == "pay"){
                category.payCnt += 1
            }
            hcMap.update(cid, category)
        }

        override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
            val map1 = this.hcMap
            val map2 = other.value
            map2.foreach{case (cid, hc) =>
                val category = map1.getOrElse(cid, HotCategory(cid))
                category.clickCnt += hc.clickCnt
                category.orderCnt += hc.orderCnt
                category.payCnt += hc.payCnt
                map1.update(cid, category)
            }
        }

        override def value: mutable.Map[String, HotCategory] = hcMap
    }
}
