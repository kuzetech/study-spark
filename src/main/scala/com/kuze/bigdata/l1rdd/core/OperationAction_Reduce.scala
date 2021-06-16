package com.kuze.bigdata.l1rdd.core

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * 聚合方法
  * 输入值和返回值类型必须一样
  */
object OperationAction_Reduce {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource1: RDD[String] = sc.parallelize(List("1","2","3","4","5","6","7","8","9"))

    val reduce: String = resource1.reduce((a,b)=>a+b)

    println(reduce)
    //789123456
    //789561234

  }
}
