package com.kuze.bigdata.l1rdd.core_map

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object OperationAction_CollectAsMap {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource: RDD[(String, Int)] = sc.parallelize(List(("a",2),("b",4),("c",6),("c",5)))

    val collectAsMap: collection.Map[String, Int] = resource.collectAsMap()

    println(collectAsMap.toBuffer)
    //ArrayBuffer((b,4), (a,2), (c,5))

  }
}
