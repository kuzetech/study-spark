package com.kuze.bigdata.l1rdd.core_map

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object OperationLazy_MapValue {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource: RDD[(String, Int)] = sc.parallelize(List(("a",2),("b",4),("c",6),("c",5)))

    val mapValues: RDD[(String, Int)] = resource.mapValues(_ + 1)

    val result: Array[(String, Int)] = mapValues.collect()

    println(result.toBuffer)
    //ArrayBuffer((a,3), (b,5), (c,7), (c,6))

  }

}
