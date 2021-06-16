package com.kuze.bigdata.l1rdd.core_double_rdd

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object OperationLazy_Subtract {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource1: RDD[(String, Int, Int)] = sc.parallelize(List(("a",1,1),("b",2,2)))

    val resource2: RDD[(String, Int, Int)] = sc.parallelize(List(("a",2,1),("b",2,2)))

    val subtract: RDD[(String, Int, Int)] = resource1.subtract(resource2)

    val result: Array[(String, Int, Int)] = subtract.collect()

    println(result.toBuffer)
    //ArrayBuffer((a,1,1))

  }
}
