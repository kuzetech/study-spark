package com.kuze.bigdata.l1rdd.core_double_rdd

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object OperationLazy_Union {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource1: RDD[String] = sc.parallelize(List("a","b","c"))

    val resource2: RDD[String] = sc.parallelize(List("1","2","3"))

    val union: RDD[String] = resource1.union(resource2)

    val collect: Array[String] = union.collect()

    println(collect.toBuffer)
    //ArrayBuffer(a, b, c, 1, 2, 3)
  }
}
