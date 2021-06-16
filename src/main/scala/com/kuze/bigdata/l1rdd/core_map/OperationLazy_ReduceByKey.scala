package com.kuze.bigdata.l1rdd.core_map

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object OperationLazy_ReduceByKey {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource: RDD[(Int, Int)] = sc.parallelize(List((1,2),(3,4),(3,6)))

    //因为是懒处理，spark在读取数据的时候会提前把相同key推到同一分区
    val reduce: RDD[(Int, Int)] = resource.reduceByKey(_ + _)

    println(reduce.getNumPartitions) // =4

    val reducePartition: RDD[(Int, Int)] = resource.reduceByKey(_ + _,2)

    println(reducePartition.getNumPartitions) // =2

    val result: Array[(Int, Int)] = reducePartition.collect()

    println(result.toBuffer)
    //ArrayBuffer((1,2), (3,10))

  }

}
