package com.kuze.bigdata.l1rdd.core_double_rdd

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * 取所有key，把相同key的数据，组成数组
  */
object OperationLazy_CoGroup {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource1: RDD[(Int, Int)] = sc.parallelize(List((1,2),(3,4),(3,6)))

    val resource2: RDD[(Int, Int)] = sc.parallelize(List((3,7),(4,5)))

    val cogroup: RDD[(Int, (Iterable[Int], Iterable[Int]))] = resource1.cogroup(resource2)

    val result: Array[(Int, (Iterable[Int], Iterable[Int]))] = cogroup.collect()

    println(result.toBuffer)
    //ArrayBuffer((4,(CompactBuffer(),CompactBuffer(5))), (1,(CompactBuffer(2),CompactBuffer())), (3,(CompactBuffer(4, 6),CompactBuffer(7))))

  }

}
