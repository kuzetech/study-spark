package com.kuze.bigdata.l1rdd.core

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * ﻿take(n)
  * 返回RDD中的n个元素，并且尝试只访问尽量少的分区
  * 因此该操作会得到一个不均衡的集合
  * 需要注意这些操作返回元素的顺序与你预期的可能不一样
  */
object OperationAction_Take {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource1: RDD[Int] = sc.parallelize(List(1,2,3,4,5,6,7,8,9))

    println(resource1.getNumPartitions)// =4

    val take: Array[Int] = resource1.take(4)

    println(take.toBuffer)
    //ArrayBuffer(1, 2, 3, 4)

  }
}
