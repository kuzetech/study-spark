package com.kuze.bigdata.l1rdd.core_partition

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.{HashPartitioner, Partitioner, SparkContext}
import org.apache.spark.rdd.RDD


/**
  * 二元连接计算符号需要更加注意分区，设定一个好的分区至少能减少一遍的数据混洗
  * map() 这样的方法会导致分区失效，尽量使用mapValues() 和 flatMapValues() 替代
  * ﻿这里列出了所有会为生成的结果 RDD 设好分区方式的操作：
  * cogroup()
  * groupWith()
  * join()
  * leftOuterJoin()
  * rightOuterJoin()
  * groupByKey()
  * reduceByKey()
  * combineByKey()
  * partitionBy()
  * sort()
  * mapValues()（如果父 RDD 有分区方式的话）
  * flatMapValues()（如果父 RDD 有分区方式的话）
  * filter()（如果父 RDD 有分区方式的话）
  *
  */
object OperationLazy_HashPartition {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val userFocusData: RDD[(Int, Array[String])] = sc.parallelize(List(
      (1, Array("www.baidu.com", "www.douyu.com")),
      (2, Array("www.bilibili.com")),
      (3, Array("www.zhihu.com"))
    ))

    val noPartitioner: Option[Partitioner] = userFocusData.partitioner

    if(noPartitioner.isDefined){
      println(s"有分区${noPartitioner}")
    }else{
      println("没有分区")
    }


    //如果不persist，将重新计算hash分区
    val userFocusDataPartition: RDD[(Int, Array[String])] = userFocusData.partitionBy(new HashPartitioner(3)).persist()

    val partitioner: Option[Partitioner] = userFocusDataPartition.partitioner

    if(partitioner.isDefined){
      println(s"有分区${partitioner}")
    }else{
      println("没有分区")
    }

    val userAccessRecord: RDD[(Int, String)] = sc.parallelize(List(
      (1, "www.baidu.com"),
      (1, "www.zhihu.com"),
      (2, "www.bilibili.com")
    ))

    //不分区的情况下，join会将所有数据集中于一台进行计算，两组数据都进行了混洗，最终计算结果也只在一台服务器上
    //分区之后，根据键值分配到不同的分区，只会混洗access的数据，最终计算结果并行在多台服务器上
    val join: RDD[(Int, (Array[String], String))] = userFocusData.join(userAccessRecord)

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
