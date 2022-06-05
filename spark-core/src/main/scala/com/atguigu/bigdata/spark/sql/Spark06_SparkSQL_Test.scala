package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession, functions}
import java.text.DecimalFormat
import scala.collection.mutable

object Spark06_SparkSQL_Test {
    def main(args: Array[String]): Unit = {
        // TODO 创建SparkSQL的运行环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")


        val spark = SparkSession.builder()
            .enableHiveSupport()
            .config("spark.sql.warehouse.dir", "hdfs://hadoop102:9820/user/hive/warehouse")
            .config(sparkConf)
            .getOrCreate()
        spark.sql("use atguigu")
        // 准备数据
        spark.sql(
            s"""
               |select * from user_visit_action
               |""".stripMargin).show(false)

        spark.sql(
            s"""
               |select * from product_info
               |""".stripMargin).show(false)


        spark.sql(
            s"""
               |select * from city_info
               |""".stripMargin).show(false)

        //3.2 需求：各区域热门商品 Top3
        //这里的热门商品是从点击量的维度来看的，计算各个区域前三大热门商品，并备注上每个商品在主要城市中的分布比例，超过两个城市用其他显示。
        spark.udf.register("my_udaf",functions.udaf(new MyUDAF))
        spark.sql(
            s"""
               |select
               |    area,
               |    product_name,
               |    click_cnt,
               |    city_detail
               |from
               |(
               |    select
               |        area,
               |        product_name,
               |        click_cnt,
               |        city_detail,
               |        row_number() over(partition by area order by click_cnt desc) as r_n
               |    from
               |    (
               |        select
               |            t1.area,
               |            t2.product_name,
               |            t1.click_cnt,
               |            t1.city_detail
               |        from
               |        (
               |            select
               |                u.click_product_id, -- 商品id
               |                c.area,  -- 地域id
               |                count(1) as click_cnt, --点击次数
               |                my_udaf(c.city_name) as  city_detail -- 城市备注
               |            from
               |                user_visit_action u
               |            left join
               |                city_info c
               |            on c.city_id = u.city_id
               |            where u.click_product_id <> '-1'
               |            group by
               |                c.area, u.click_product_id
               |        ) t1
               |        left join
               |        product_info t2
               |        on t1.click_product_id = t2.product_id
               |    ) t
               |) t
               |where r_n <= 3
               |""".stripMargin)



        // TODO 关闭环境
        spark.close()

    }

    case class CityDetail(var totalCnt:Long, cityCntMap:mutable.Map[String,Long])
    class MyUDAF extends Aggregator[String, CityDetail,String]{

        override def zero: CityDetail = CityDetail(0L, mutable.Map[String,Long]())

        override def reduce(b: CityDetail, a: String): CityDetail = {
            b.totalCnt += 1L
            b.cityCntMap.update(a, b.cityCntMap.getOrElse(a,0L) + 1L)
            b
        }

        override def merge(b1: CityDetail, b2: CityDetail): CityDetail = {
            b1.totalCnt += b2.totalCnt
            b2.cityCntMap.foldLeft(b1.cityCntMap)((m,e) => {
                m.update(e._1, m.getOrElse(e._1,0L) + e._2)
                m
            })
            b1
        }

        override def finish(reduction: CityDetail): String = {
            val topCityDetail = reduction.cityCntMap.toList.sortBy(_._2)(Ordering.Long.reverse)
            val totalCnt = reduction.totalCnt

            var res:String = null
            if(topCityDetail.size <= 2){
                res = topCityDetail.map(topCityDetail => {
                    s"${topCityDetail._1} ${formatNum(topCityDetail._2.toDouble / totalCnt)}"
                }).mkString(", ")
            }else{
                val top2CityDetail = topCityDetail.take(2)
                val otherCount = totalCnt - top2CityDetail.map(_._2).sum
                res = top2CityDetail.map(topCityDetail => {
                    s"${topCityDetail._1} ${formatNum(topCityDetail._2.toDouble / totalCnt)}"
                }).mkString(", ")+", 其他 "+ formatNum(otherCount.toDouble / totalCnt)

            }
            res
        }

        def formatNum(d:Double) ={

            val dataFormat = new DecimalFormat("0.00%")
            dataFormat.format(d)
        }

        override def bufferEncoder: Encoder[CityDetail] = Encoders.product

        override def outputEncoder: Encoder[String] = Encoders.STRING
    }

}
