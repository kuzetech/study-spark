package com.kuze.bigdata.l1rdd.core_cache

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object OperationCache_CheckPoint {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    sc.setCheckpointDir("/dirOrHDFS")

    val resource1: RDD[String] = sc.parallelize(List("a","b","c"))

    //迭代计算，不容许数据丢失，对速度要求不高的情况下,缓存中间结果到HDFS
    resource1.checkpoint()

    val collect: Array[String] = resource1.collect()

    println(collect.toBuffer)

  }
}
