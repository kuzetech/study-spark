package com.kuze.bigdata.l5sharestore

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * 优化的话
  * 选择合适的序列化方式
  */
object Broadcast {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val userFocusMap = Map(
      1 -> Array("www.baidu.com", "www.douyu.com"),
      2 -> Array("www.bilibili.com"),
      3 -> Array("www.zhihu.com")
    )

    val broadcastRef: Broadcast[Map[Int, Array[String]]] = sc.broadcast(userFocusMap)


    val userAccessRecord: RDD[(Int, String)] = sc.parallelize(List(
      (1, "www.baidu.com"),
      (1, "www.zhihu.com"),
      (2, "www.bilibili.com")
    ))

    val join: RDD[(Int, (Array[String], String))] = userAccessRecord.map(o=>{
      val map: Map[Int, Array[String]] = broadcastRef.value
      (o._1,(map.get(o._1).get,o._2))
    })

    val filter: RDD[(Int, (Array[String], String))] = join.filter(item => {
      !item._2._1.contains(item._2._2)
    })

    val map: RDD[((Int, String), Int)] = filter.map(record => {
      ((record._1, record._2._2), 1)
    })

    val reduce: RDD[((Int, String), Int)] = map.reduceByKey(_ + _)

    val collect: Array[((Int, String), Int)] = reduce.collect()

    println(collect.toBuffer)

  }

}
