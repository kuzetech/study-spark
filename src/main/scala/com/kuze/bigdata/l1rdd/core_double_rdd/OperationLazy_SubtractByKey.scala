package com.kuze.bigdata.l1rdd.core_double_rdd

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * 差集，仅在左边存在的key
  */
object OperationLazy_SubtractByKey {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource1: RDD[(Int, Int)] = sc.parallelize(List((1,2),(3,4),(3,6)))

    val resource2: RDD[(Int, Int)] = sc.parallelize(List((3,7),(4,5)))

    val subtract: RDD[(Int, Int)] = resource1.subtractByKey(resource2)

    val result: Array[(Int, Int)] = subtract.collect()

    println(result.toBuffer)
    //ArrayBuffer((1,2))

  }

}
