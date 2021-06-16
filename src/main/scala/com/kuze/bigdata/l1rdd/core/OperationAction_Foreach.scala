package com.kuze.bigdata.l1rdd.core

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object OperationAction_Foreach {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource1: RDD[Int] = sc.parallelize(List(1,2,3,4,5,6,7,8,9))

    resource1.foreach(item =>{

      println(item)

    })
    /*
    3
    5
    7
    1
    8
    6
    4
    9
    2
    */

  }
}
