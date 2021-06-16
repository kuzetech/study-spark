package com.kuze.bigdata.l1rdd.core

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * takeSample(withReplacement, num, seed)
  * ﻿在驱动器程序中对我们的数据进行采样
  * 函数可以让我们从数据中获取一个采样，并指定是否替换
  */
object OperationAction_TakeSample {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource1: RDD[Int] = sc.parallelize(List(1,2,3,4,5,6,7,8,9))

    println(resource1.getNumPartitions)// =4

    val takeSample: Array[Int] = resource1.takeSample(false,4)

    println(takeSample.toBuffer)
    //ArrayBuffer(2, 8, 9, 5)

  }
}
