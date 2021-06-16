package com.kuze.bigdata.l1rdd.streaming_core

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 有状态操作：依赖之前批次的数据
  * window
  */
object TransformOperation_Status_Window {

  def main(args: Array[String]): Unit = {

    //离线任务是创建SparkContext，现在要实现实时计算，用StreamingContext

    val conf = new SparkConf().setAppName("SteamingWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //StreamingContext是对SparkContext的包装，包了一层就增加了实时的功能
    //第二个参数是小批次产生的时间间隔
    val ssc = new StreamingContext(sc, Milliseconds(500))

    //有了StreamingContext，就可以创建SparkStreaming的抽象了DSteam
    //从一个socket端口中读取数据
    //在Linux上用yum安装nc
    //yum install -y nc
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.1.207", 8888)

    /**
      * window操作
      */
    val window: DStream[String] = lines.window(Milliseconds(1000),Milliseconds(1000))
    val words: DStream[String] = window.flatMap(_.split(" "))
    val wordAndOne: DStream[(String, Int)] = words.map((_, 1))
    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)
    reduced.print()


    /**
      * ﻿尽管可以使用 window() 写出所有的窗口操作，
      * Spark Streaming 还是提供了一些其他的窗口操作，让用户可以高效而方便地使用。
      * 首先，reduceByWindow() 和 reduceByKeyAndWindow() 让我们可以对每个窗口更高效地进行归约操作。
      * 它们接收一个归约函数，在整个窗口上执行，比如 +。
      * 除此以外，它们还有一种特殊形式，通过只考虑新进入窗口的数据和离开窗口的数据，让 Spark 增量计算归约结果。
      * 这种特殊形式需要提供归约函数的一个逆函数，示例如下：
      *
      */
    val words1: DStream[String] = lines.flatMap(_.split(" "))
    val wordAndOne1: DStream[(String, Int)] = words1.map((_, 1))
    val reduced1 = wordAndOne1.reduceByWindow(
      {(x, y) => (x._1 + y._1,x._2 + y._2)},  // 加上新进入窗口的批次中的元素
      {(x, y) => (x._1.substring(0,x._1.length-y._1.length),x._2 - y._2)},  // 移除离开窗口的老批次中的元素
      Milliseconds(1000), // 窗口时长
      Milliseconds(1000)  // 滑动步长
    )
    val reduced2 = wordAndOne1.reduceByKeyAndWindow(
      {(x, y) => x + y},  // 加上新进入窗口的批次中的元素
      {(x, y) => x - y},  // 移除离开窗口的老批次中的元素
      Milliseconds(1000), // 窗口时长
      Milliseconds(1000)  // 滑动步长
    )
    reduced1.print()

    /**
      * 其他操作
      * wordAndOne1.countByWindow()
      * wordAndOne1.countByValueAndWindow()
      */

    //启动sparksteaming程序
    ssc.start()
    //等待优雅的退出
    ssc.awaitTermination()

  }


}
