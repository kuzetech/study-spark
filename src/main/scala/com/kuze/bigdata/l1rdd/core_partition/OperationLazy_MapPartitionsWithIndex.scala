package com.kuze.bigdata.l1rdd.core_partition

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object OperationLazy_MapPartitionsWithIndex {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource: RDD[String] = sc.parallelize(List("1","2","3","4","5","6","7","8","9"))

    val list: RDD[(Int, String)] = resource.mapPartitionsWithIndex((num, partitions) => {

      println(s"分区号是${num},包含的值如下：")

      var list = List[(Int, String)]()

      partitions.foreach(o => {
        list = list.+:((num, o))
      })

      list.iterator
    })

    val collect: Array[(Int, String)] = list.collect()

    println(collect.toBuffer)


  }

}
