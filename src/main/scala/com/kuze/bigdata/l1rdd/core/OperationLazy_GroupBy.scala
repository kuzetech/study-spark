package com.kuze.bigdata.l1rdd.core

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object OperationLazy_GroupBy {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource: RDD[String] = sc.parallelize(List("hello world","test"))

    val group: RDD[(String, Iterable[String])] = resource.groupBy(_.split(" ")(0))

    val result: Array[(String, Iterable[String])] = group.collect()

    println(result.toBuffer)
    //ArrayBuffer((hello,CompactBuffer(hello world)), (test,CompactBuffer(test)))

  }

}
