package com.kuze.bigdata.l1rdd.core_map

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object OperationLazy_FlatMapValue {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource: RDD[(Int, String)] = sc.parallelize(List((1,"hello world")))

    val flatMapValues: RDD[(Int, String)] = resource.flatMapValues(_.split(" "))

    val result: Array[(Int, String)] = flatMapValues.collect()

    println(result.toBuffer)
    //ArrayBuffer((1,hello), (1,world))
    OperationLazy_ReduceByKey

  }

}
