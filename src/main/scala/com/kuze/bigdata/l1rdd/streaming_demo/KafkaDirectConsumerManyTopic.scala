package com.kuze.bigdata.l1rdd.streaming_demo

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object KafkaDirectConsumerManyTopic {

  def main(args: Array[String]): Unit = {

    //创建SparkConf
    val conf = new SparkConf().setAppName("KafkaDirectConsumerManyTopic").setMaster("local[4]")

    //创建SparkStreaming，并设置间隔时间
    val streamingContext = new StreamingContext(conf, Milliseconds(5000))

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
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](Array("test1","test2"), kafkaParams)
    )

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

      val result: Array[(String, Int)] = reduced.collect()

      println(result.toBuffer)

      //更新偏移量
      offsetRanges.foreach { offsetRange =>
        println("partition : " + offsetRange.partition + " fromOffset:  " + offsetRange.fromOffset + " untilOffset: " + offsetRange.untilOffset)
        kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(Array(offsetRange))
      }
    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
