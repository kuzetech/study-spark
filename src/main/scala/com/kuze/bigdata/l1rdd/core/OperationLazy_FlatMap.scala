package com.kuze.bigdata.l1rdd.core

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object OperationLazy_FlatMap {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource: RDD[String] = sc.parallelize(List("hello world","test"))

    val words: RDD[String] = resource.flatMap(text=>text.split(" "))

    val collect: Array[String] = words.collect()

    println(collect.toBuffer)
    //ArrayBuffer(hello, world, test)

  }

}
