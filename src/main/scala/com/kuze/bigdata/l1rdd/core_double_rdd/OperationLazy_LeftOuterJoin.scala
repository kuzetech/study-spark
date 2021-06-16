package com.kuze.bigdata.l1rdd.core_double_rdd

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * 取左边所有的key，按key结合右边的value
  */
object OperationLazy_LeftOuterJoin {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource1: RDD[(Int, Int)] = sc.parallelize(List((1,2),(3,4),(3,6)))

    val resource2: RDD[(Int, Int)] = sc.parallelize(List((3,7),(4,5)))

    val left: RDD[(Int, (Int, Option[Int]))] = resource1.leftOuterJoin(resource2)

    left.foreach(item=>{
      if(item._2._2.isDefined){
        println(item._2._2.get)
      }
    })

    val result: Array[(Int, (Int, Option[Int]))] = left.collect()

    println(result.toBuffer)
    //ArrayBuffer((1,(2,None)), (3,(4,Some(7))), (3,(6,Some(7))))

  }

}
