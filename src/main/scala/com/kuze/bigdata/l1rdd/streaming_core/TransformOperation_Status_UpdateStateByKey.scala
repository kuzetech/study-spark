package com.kuze.bigdata.l1rdd.streaming_core

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 有状态操作：依赖之前批次的数据
  * updateStateByKey
  * ﻿有时，我们需要在 DStream 中跨批次维护状态（例如跟踪用户访问网站的会话）。
  * 针对这种情况，updateStateByKey() 为我们提供了对一个状态变量的访问，用于键值对形式的 DStream。
  * 给定一个由（键，事件）对构成的 DStream
  * 并传递一个指定如何根据新的事件更新每个键对应状态的函数
  * 它可以构建出一个新的 DStream，其内部数据为（键，状态）对。
  * 例如，在网络服务器日志中，事件可能是对网站的访问，此时键是用户的 ID。
  * 使用 updateStateByKey() 可以跟踪每个用户最近访问的 10 个页面。
  * 这个列表就是“状态”对象，我们会在每个事件到来时更新这个状态。
  */
object TransformOperation_Status_UpdateStateByKey {

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

    //单词和一组合在一起
    val wordAndOne: DStream[(String, Int)] = words.map((_, 1))

    //聚合
    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)

    val addFunction = (currValues: Seq[Int], preValueState: Option[Int]) => {
      val currentSum = currValues.sum
      val previousSum = preValueState.getOrElse(1)
      Some(currentSum + previousSum)
    }

    val updateState: DStream[(String, Int)] = reduced.updateStateByKey(addFunction)

    //打印结果(Action)
    updateState.print()

    //启动sparksteaming程序
    ssc.start()
    //等待优雅的退出
    ssc.awaitTermination()

  }


}
