package com.atguigu.bigdata.spark.core.test

class SubTask extends Serializable {
    // 数据
    var datas:List[Int] = _

    var logic : Int => Int = _

    // 计算
    def compute()={
        datas.map(logic)
    }

}
