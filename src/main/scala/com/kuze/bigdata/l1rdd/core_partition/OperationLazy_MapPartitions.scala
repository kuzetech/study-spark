package com.kuze.bigdata.l1rdd.core_partition

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object OperationLazy_MapPartitions {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource: RDD[String] = sc.parallelize(List("1","2","3","4","5","6","7","8","9"))

    val value: RDD[Int] = resource.mapPartitions(partitions => {
      val sumCount = Array(0, 0)
      partitions.foreach(item => {
        sumCount(0) = sumCount(0) + item.toInt
        sumCount(1) = sumCount(1) + 1
      })
      sumCount.iterator
    })

    val collect: Array[Int] = value.collect()

    println(collect.toBuffer)


  }

}
