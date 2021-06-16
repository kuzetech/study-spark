package com.kuze.bigdata.l1rdd.streaming_core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
  * 无状态操作：不依赖之前批次的数据
  */
object TransformOperation_StatusLess {

  // 一元操作符
  // map
  // flatMap
  // filter
  // repartition
  // reduceByKey
  // groupByKey

  // 二元操作符
  // cogroup
  // join
  // leftOuterJoin
  // union
  // 等等

  // 以上的操作符对象都是DSteam
  // 如果这些无状态转化操作不够用，DStream还提供了一个叫作transform()的高级操作符，可以让你直接操作其内部的RDD
  // 多数时候可以重用针对RDD的方法

  def main(args: Array[String]): Unit = {

    //离线任务是创建SparkContext，现在要实现实时计算，用StreamingContext

    val conf = new SparkConf().setAppName("SteamingWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //StreamingContext是对SparkContext的包装，包了一层就增加了实时的功能
    //第二个参数是小批次产生的时间间隔
    val ssc = new StreamingContext(sc, Milliseconds(5000))

    //有了StreamingContext，就可以创建SparkStreaming的抽象了DSteam
    //从一个socket端口中读取数据
    //在Linux上用yum安装nc
    //yum install -y nc
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.1.207", 8888)

    //对DSteam进行操作，你操作这个抽象（代理，描述），就像操作一个本地的集合一样
    //切分压平
    val words: DStream[String] = lines.flatMap(_.split(" "))

    /**
      * DStream操作方式
      */
    //单词和一组合在一起
    val wordAndOne1: DStream[(String, Int)] = words.map((_, 1))
    //聚合
    val reduced1: DStream[(String, Int)] = wordAndOne1.reduceByKey(_+_)
    //打印结果(Action)
    reduced1.print()

    /**
      * 如果流转化操作不够用，可以使用transform操作
      * 多个流RDD的时候可以使用transformWith
      */
    val wordAndOne2: DStream[(String, Int)] = words.transform((rdd, time) => {
      //可以做一些批次时间的判断之类的操作
      println(time)
      rdd.map((_, 1))
    })
    val reduced2: DStream[(String, Int)] = wordAndOne2.reduceByKey(_+_)
    reduced2.print()

    /**
      * 以下方法用﻿来整合与转化多个DStream
      * StreamingContext.transform操作
      * ﻿DStream.transformWith(otherStream, func)
      */

    //启动sparksteaming程序
    ssc.start()
    //等待优雅的退出
    ssc.awaitTermination()

  }


}
