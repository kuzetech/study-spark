package com.kuze.bigdata.l1rdd.core_double_rdd

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * 抽取出 RDD a 和 RDD b 中的公共数据
  */
object OperationLazy_Intersection {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource1: RDD[Int] = sc.parallelize(List(1,3,5))

    val resource2: RDD[Int] = sc.parallelize(List(3,4))

    val intersection: RDD[(Int)] = resource1.intersection(resource2)

    val result: Array[Int] = intersection.collect()

    println(result.toBuffer)
    //ArrayBuffer(3)

  }

}
