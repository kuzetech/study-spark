package com.kuze.bigdata.l1rdd.core

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object OperationAction_ForeachPartition {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource1: RDD[Int] = sc.parallelize(List(1,2,3,4,5,6,7,8,9))

    resource1.foreachPartition(partition =>{
      //该部分代码在同一个分区内仅执行一次
      println(partition.length)
      partition.foreach(item =>{
        //该部分代码对每一条记录执行一次
        println(item)
      })
    })

  }
}
