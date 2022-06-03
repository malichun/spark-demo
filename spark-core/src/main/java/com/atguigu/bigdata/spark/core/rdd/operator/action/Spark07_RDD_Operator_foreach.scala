package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_foreach {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)

        val user = new User()

        // SparkException: Task not serializable
        // RDD算子中传递的函数会包含闭包操作, 那么就会进行检测功能
        // 闭包检测功能
        rdd.foreach(num => {
            println("age=" + (user.age + num))
        })

        sc.stop()

    }

    // class User extends Serializable{
    // 样例类在编译时, 会自动混入序列化特质(实现序列化接口)
    case class User() {
        var age:Int = 30
    }
}
