package com.kuze.bigdata.l1rdd.core_map

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * 聚合方法
  * 可以自定义合并行为
  */
object OperationLazy_CombineByKey {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource: RDD[(String, Int)] = sc.parallelize(List(("a",2),("b",4),("c",6),("c",5)))

    //因为是懒处理，spark在读取数据的时候会提前把相同key推到同一分区
    val combine: RDD[(String, (Int, Int))] = resource.combineByKey(
      (value: Int) => (value, 1),
      (preResult: (Int, Int), nextValue: Int) => (preResult._1 + nextValue, preResult._2 + 1),
      (preResult1: (Int, Int), preResult2: (Int, Int)) => (preResult1._1 + preResult2._1, preResult1._2 + preResult2._2)
    )

    val result: Array[(String, (Int, Int))] = combine.collect()

    println(result.toBuffer)
    //ArrayBuffer((a,(2,1)), (b,(4,1)), (c,(11,2)))

  }

}
