package com.kuze.bigdata.l0utils

import org.apache.spark.SparkConf

object SparkContextUtils {

  def getDefaultSparkConf():SparkConf = {
    val conf = new SparkConf().setAppName("test").setMaster("local[4]")
    conf
  }


}
