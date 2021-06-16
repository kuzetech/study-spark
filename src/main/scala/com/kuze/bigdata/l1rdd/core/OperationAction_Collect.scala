package com.kuze.bigdata.l1rdd.core

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * collect()
  * ﻿由于需要将数据复制到驱动器进程中
  * 要求驱动器进程内存必须能容得下所有数据
  */
object OperationAction_Collect {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource1: RDD[Int] = sc.parallelize(List(1,2,3,4,5,6,7,8,9))

    println(resource1.getNumPartitions)// =4

    val collect: Array[Int] = resource1.collect()

    println(collect.toBuffer)
    //ArrayBuffer(1, 2, 3, 4, 5, 6, 7, 8, 9)

  }
}
