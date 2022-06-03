package com.atguigu.bigdata.spark.core.req

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Req1_HotCategoryTop10Analysis {
    def main(args: Array[String]): Unit = {
        // TODO: Top10热门品类

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
        val sc = new SparkContext(sparkConf)

        // 1. 读取原始的日志数据
        val actionRDD = sc.textFile("datas/user_visit_action.txt")

        // 2. 统计品类的点击数量 (品类id, 点击数量)
        val clickActionRDD = actionRDD.filter(action => {
            val datas = action.split("_")
            datas(6) != "-1"
        })

        val clickCountRDD = clickActionRDD.map(action => {
            val datas = action.split("_")
            (datas(6), 1)
        }).reduceByKey(_ + _)

        // 3. 统计品类的下单数量 (品类id, 下单数量)
        val orderActionRDD = actionRDD.filter(action => {
            val datas = action.split("_")
            datas(8) != "null"
        })
        // orderid => 1,2,3
        // [(1,1),(2,1),(3,1)]
        val orderCountRDD = orderActionRDD.flatMap(action => {
            val datas = action.split("_")
            val cids = datas(8).split(",")
            cids.map(id => (id, 1))
        }).reduceByKey(_ + _)


        // 4. 统计品类的支付数量 (品类id, 支付数量)
        val payActionRDD = actionRDD.filter(action => {
            val datas = action.split("_")
            datas(10) != "null"
        })
        // orderid => 1,2,3
        // [(1,1),(2,1),(3,1)]
        val payCountRDD = payActionRDD.flatMap(action => {
            val datas = action.split("_")
            val cids = datas(10).split(",")
            cids.map(id => (id, 1))
        }).reduceByKey(_ + _)


        // 5. 将品类进行排序, 并且取前10名
        // 点击数量的排序, 下单数量排序, 支付数量的排序
        // 元祖的排序: 先比较第一个,再比较第二个, 后再比较第三个, 依次类推
        // (品类ID, (点击数量, 下单数量, 支付数量))
        // cogroup = connect + group
        val cogroupRDD = clickCountRDD.cogroup(orderCountRDD, payCountRDD)

        val analysisRDD = cogroupRDD.mapValues {
            case (clickIter, orderIter, payIter) => {
                var clickCnt = 0
                if (clickIter.iterator.hasNext) {
                    clickCnt = clickIter.iterator.next()
                }

                var orderCnt = 0
                if (orderIter.iterator.hasNext) {
                    orderCnt = orderIter.iterator.next()
                }

                var payCnt = 0
                if (payIter.iterator.hasNext) {
                    payCnt = payIter.iterator.next()
                }
                (clickCnt, orderCnt, payCnt)
            }
        }

        val result = analysisRDD.sortBy(_._2, false).take(10)


        // 6.将结果采集到控制台打印
        result.foreach(println)

        sc.stop()

    }
}
