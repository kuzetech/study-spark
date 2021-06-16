package com.kuze.bigdata.l1rdd.core

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * 在分区中进行采样
  */
object OperationLazy_Sample {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource1: RDD[String] = sc.parallelize(List("a","b","c","d","e"))

    val sample: RDD[String] = resource1.sample(false,0.5)

    val collect: Array[String] = sample.collect()

    println(collect.toBuffer)
    //不确定的结果

  }
}
