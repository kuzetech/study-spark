package com.kuze.bigdata.l1rdd.core_map

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * ﻿group完全可以使用reduce进行代替
  * 例如，rdd.reduceByKey(func) 与 rdd.groupByKey().mapValues(value => value.reduce(func)) 等价
  * 但是reduce更为高效，因为它避免了为每个键创建存放值的列表的步骤
  */
object OperationLazy_GroupByKey {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource: RDD[(Int, Int)] = sc.parallelize(List((1,2),(3,4),(3,6)))

    //因为是懒处理，spark在读取数据的时候会提前把相同key推到同一分区
    val group: RDD[(Int, Iterable[Int])] = resource.groupByKey()

    val result: Array[(Int, Iterable[Int])] = group.collect()

    println(result.toBuffer)
    //ArrayBuffer((1,CompactBuffer(2)), (3,CompactBuffer(4, 6)))

  }

}
