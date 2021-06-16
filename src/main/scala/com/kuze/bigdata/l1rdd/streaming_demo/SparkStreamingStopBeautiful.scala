package com.kuze.bigdata.l1rdd.streaming_demo

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/**
  * hdfs dfs -ls /tmp/spark-test
  * hdfs dfs -touchz /tmp/spark-test/stop-spark
  * hdfs dfs -rm /tmp/spark-test/stop-spark
  */
object SparkStreamingStopBeautiful {

  val hdfsPath = "hdfs://managenode-235:8020"
  val shutdownMarker = "/tmp/spark-test/stop-spark"
  var stopFlag: Boolean = false

  def main(args: Array[String]): Unit = {

    //创建SparkConf
    val conf = new SparkConf().setAppName("SparkStreamingStopBeautiful").setMaster("local[4]")

    //创建SparkStreaming，并设置间隔时间
    val ssc = new StreamingContext(conf, Milliseconds(5000))

    //准备kafka的参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "managenode-237:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "g001",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Array("SparkStreamingStopBeautiful"), kafkaParams)
    )

    //直连方式只有在KafkaDStream的RDD中才能获取偏移量，那么就不能到调用DStream的Transformation
    //所以只能子在kafkaStream调用foreachRDD，获取RDD的偏移量，然后就是对RDD进行操作了
    //依次迭代KafkaDStream中的KafkaRDD
    //直连方式累加只能使用第三方存储，最快的使用key，value形式的nosql数据库，最牛的是redis
    kafkaStream.foreachRDD { kafkaRDD =>

      //只有KafkaRDD可以强转成HasOffsetRanges，并获取到偏移量
      val offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges

      //对RDD进行操作，触发Action
      val lines: RDD[String] = kafkaRDD.map(_.value())

      //将每行文本拆成单词
      val words = lines.flatMap(_.split(" "))

      //单词和一组合在一起
      val wordAndOne = words.map((_, 1))

      //聚合
      val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)

      val collect: Array[(String, Int)] = reduced.collect()

      println(collect.toBuffer)

      //更新偏移量
      kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }
    //开始执行任务
    ssc.start()

    var isStopped = false
    while (!isStopped) {
      //println("calling awaitTerminationOrTimeout")
      //等待执行器停止。执行过程中发生的任何异常都会在此线程中抛出，如果执行器停止了返回true，
      //线程等待超时长，当超过timeout时间后，会监测ExecutorService是否已经关闭，若关闭则返回true，否则返回false。
      isStopped = ssc.awaitTerminationOrTimeout(10000)
      /*if (isStopped) {
        println("confirmed! The streaming context is stopped. Exiting application...")
      } else {
        println("Streaming App is still running. Timeout...")
      }*/
      //判断文件夹是否存在
      if (!stopFlag) {
        //开始检查hdfs是否有stop-spark文件夹
        val fs: FileSystem = FileSystem.get(new URI(hdfsPath), new Configuration(), "root")
        //如果有返回true，如果没有返回false
        stopFlag = fs.exists(new Path(shutdownMarker))
      }
      if (!isStopped && stopFlag) {
        println("stopping spark right now")
        //第一个true：停止相关的SparkContext。无论这个流媒体上下文是否已经启动，底层的SparkContext都将被停止。
        //第二个true：则通过等待所有接收到的数据的处理完成，从而优雅地停止。
        ssc.stop(true, true)
        println("spark is stopped!!!!!!!")
      }
    }

  }

}

