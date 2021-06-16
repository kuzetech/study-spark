package com.kuze.bigdata.l1rdd.core_statistics

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * 聚合方法
  * 拥有初始值,仅分组聚合时加入初始值
  * 输入值和返回值类型可以不一样
  */
object OperationAction_AggregateByKey {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource1 = sc.parallelize(List(("a",1),("b",2),("c",3),("d",4),("e",5),("f",6)))

    val aggregateByKey: RDD[(String, Int)] = resource1.aggregateByKey((0))(
      (preResult, nextValue) => {
        (preResult + nextValue)
      },
      (partitionResult1, partitionResult2) => {
        (partitionResult1 + partitionResult2)
      }
    )

    println(aggregateByKey.collect().toBuffer)

  }
}
