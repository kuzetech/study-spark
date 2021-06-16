package com.kuze.bigdata.l1rdd.core

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * 聚合方法
  * 拥有初始值
  * 输入值和返回值类型必须一样
  */
object OperationAction_Fold {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource1: RDD[String] = sc.parallelize(List("1","2","3","4","5","6","7","8","9"))

    println(resource1.getNumPartitions)

    val reduce: String = resource1.fold("a")((preResult, nextValue)=> preResult + nextValue)

    println(reduce)
    //aa12a56a789a34
    //aa34a56a789a12
    //一共五个a,4个分区初始值各一个，驱动程序再加一个

  }
}
