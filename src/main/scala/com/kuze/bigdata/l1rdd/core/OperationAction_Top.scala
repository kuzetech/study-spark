package com.kuze.bigdata.l1rdd.core

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * top()
  * 从 RDD 中获取前几个元素
  * 会使用数据的默认顺序
  * 也可以提供比较函数
  */
object OperationAction_Top {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource1: RDD[Int] = sc.parallelize(List(1,2,3,4,5,6,7,8,9))

    println(resource1.getNumPartitions)// =4

    val top: Array[Int] = resource1.top(4)

    println(top.toBuffer)
    //ArrayBuffer(9, 8, 7, 6)

  }
}
