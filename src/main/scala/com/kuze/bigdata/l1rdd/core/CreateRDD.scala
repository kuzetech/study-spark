package com.kuze.bigdata.l1rdd.core

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object CreateRDD {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    /**
      * 从文件中生成RDD
      * 默认两个分区
      */
    val fileResource: RDD[String] = sc.textFile("/Users/baiye/Resources/TestSpark/src/main/resources/cclog")

    println(fileResource.getNumPartitions)

    /**
      * 从集合中生成RDD
      *
      */
    val paraResource1: RDD[String] = sc.parallelize(List("hello world","test"))

    println(paraResource1.getNumPartitions) // =4，平均到每个执行器

    val paraResource2: RDD[(String, Int)] = sc.parallelize(List(("a",1),("b",2),("c",3),("a",1)))

    println(paraResource2.getNumPartitions) // =4，平均到每个执行器

  }

}
