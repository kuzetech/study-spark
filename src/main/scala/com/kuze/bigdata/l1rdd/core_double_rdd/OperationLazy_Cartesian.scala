package com.kuze.bigdata.l1rdd.core_double_rdd

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object OperationLazy_Cartesian {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource1: RDD[String] = sc.parallelize(List("a","b","c"))

    val resource2: RDD[Int] = sc.parallelize(List(1,2,3))

    val cartesian: RDD[(String, Int)] = resource1.cartesian(resource2)

    val collect: Array[(String, Int)] = cartesian.collect()

    println(collect.toBuffer)
    //ArrayBuffer((a,1), (a,2), (a,3), (b,1), (b,2), (b,3), (c,1), (c,2), (c,3))
  }
}
