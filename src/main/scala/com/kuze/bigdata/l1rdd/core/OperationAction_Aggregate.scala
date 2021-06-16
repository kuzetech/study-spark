package com.kuze.bigdata.l1rdd.core

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * 聚合方法
  * 拥有初始值,分组聚合时加入初始值，总聚合时还会再加入初始值
  * 输入值和返回值类型可以不一样
  */
object OperationAction_Aggregate {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource1: RDD[Int] = sc.parallelize(List(1,2,3,4,5,6,7,8,9))

    val aggregate: (Int, Int) = resource1.aggregate((0, 0))(
      //(0,0) + 1
      (preResult, nextValue) => {
        (preResult._1 + nextValue, preResult._2 + 1)
      },
      //(0,0) + (0,0)
      (partitionResult1, partitionResult2) => {
        (partitionResult1._1 + partitionResult2._1, partitionResult1._2 + partitionResult2._2)
      }
    )

    val avg: Int = aggregate._1 / aggregate._2

    println(avg)
    //5

  }
}
