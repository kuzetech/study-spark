package com.kuze.bigdata.l1rdd.core_partition

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * 重新分区
  * 该方法比repartition更优
  */
object OperationLazy_Coalesce {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource: RDD[(Int, Int)] = sc.parallelize(List((1,2),(3,4),(3,6)))

    println(resource.getNumPartitions) // =4

    val coalesce: RDD[(Int, Int)] = resource.coalesce(2,false)

    println(coalesce.getNumPartitions) // =2

    val result: Array[(Int, Int)] = coalesce.collect()

    println(result.toBuffer)
    //ArrayBuffer((1,2), (3,4), (3,6))

  }

}
