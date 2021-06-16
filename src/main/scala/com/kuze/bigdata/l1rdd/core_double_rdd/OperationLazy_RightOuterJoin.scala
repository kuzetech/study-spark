package com.kuze.bigdata.l1rdd.core_double_rdd

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * 取右边所有的key，按key结合左边的value
  */
object OperationLazy_RightOuterJoin {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource1: RDD[(Int, Int)] = sc.parallelize(List((1,2),(3,4),(3,6)))

    val resource2: RDD[(Int, Int)] = sc.parallelize(List((3,7),(4,5)))

    val right: RDD[(Int, (Option[Int], Int))] = resource1.rightOuterJoin(resource2)

    val result: Array[(Int, (Option[Int], Int))] = right.collect()

    println(result.toBuffer)
    //ArrayBuffer((4,(None,5)), (3,(Some(4),7)), (3,(Some(6),7)))

  }

}
