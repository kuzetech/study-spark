package com.kuze.bigdata.l1rdd.core

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * takeOrdered(num)(ordering)
  */
object OperationAction_TakeOrdered {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource1: RDD[Int] = sc.parallelize(List(1,2,3,4,5,6,7,8,9))

    println(resource1.getNumPartitions)// =4

    val takeOrdered: Array[Int] = resource1.takeOrdered(4)

    println(takeOrdered.toBuffer)
    //ArrayBuffer(1, 2, 3, 4)

  }
}
