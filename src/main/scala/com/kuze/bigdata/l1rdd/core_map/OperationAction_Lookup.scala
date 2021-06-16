package com.kuze.bigdata.l1rdd.core_map

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object OperationAction_Lookup {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource: RDD[(String, Int)] = sc.parallelize(List(("a",2),("b",4),("c",6),("c",5)))

    val lookup: Seq[Int] = resource.lookup("c")

    println(lookup.toBuffer)
    //ArrayBuffer(6, 5)

  }
}
