package com.kuze.bigdata.l1rdd.core_map

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object OperationLazy_KeysValues {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource: RDD[(Int, Int)] = sc.parallelize(List((1,2),(3,4),(3,6)))

    val keys: RDD[Int] = resource.keys

    val values: RDD[Int] = resource.values

    val result1: Array[Int] = keys.collect()

    println(result1.toBuffer)

    val result2: Array[Int] = values.collect()

    println(result2.toBuffer)

  }

}
