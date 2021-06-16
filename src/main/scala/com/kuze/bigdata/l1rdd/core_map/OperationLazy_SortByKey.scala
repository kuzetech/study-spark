package com.kuze.bigdata.l1rdd.core_map

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * 只会按照key排序，value是无序的
  */
object OperationLazy_SortByKey {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource: RDD[(Int, Int)] = sc.parallelize(List((1,2),(3,4),(3,6)))

    val ascSort: RDD[(Int, Int)] = resource.sortByKey(true)

    val ascResult: Array[(Int, Int)] = ascSort.collect()

    println(ascResult.toBuffer)
    //ArrayBuffer((1,2), (3,4), (3,6))

    val descSort: RDD[(Int, Int)] = resource.sortByKey(false)

    val descResult: Array[(Int, Int)] = descSort.collect()

    println(descResult.toBuffer)
    //ArrayBuffer((3,4), (3,6), (1,2))

  }

}
