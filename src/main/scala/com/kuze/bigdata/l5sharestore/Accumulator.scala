package com.kuze.bigdata.l5sharestore

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CollectionAccumulator

/**
  * 累加器如果不位于行为操作中，spark异常导致的重新运行任务等等会影响最终结果
  */
object Accumulator {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource: RDD[String] = sc.parallelize(List("","b","c","","e"))

    val strAccumulator: CollectionAccumulator[String] = sc.collectionAccumulator[String]("test")

    resource.foreach(line=>{
      if(!line.equals("")){
        strAccumulator.add(line)
        println(strAccumulator.toString())
      }
    })

    println(strAccumulator.toString())

  }

}
