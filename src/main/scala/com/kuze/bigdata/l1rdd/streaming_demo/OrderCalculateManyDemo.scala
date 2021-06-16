package com.kuze.bigdata.l1rdd.streaming_demo

import com.kuze.bigdata.l0utils.{IpUtils, OrderUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object OrderCalculateManyDemo {


  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("OrderCalculateMany").setMaster("local[4]")

    val ssc = new StreamingContext(conf,Seconds(5))

    val sc: SparkContext = ssc.sparkContext

    val ipRules: Array[(Long, Long, String)] = IpUtils.readRules("/Users/baiye/Downloads/ip.txt")

    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(ipRules)

    val topics = Array("test")

    val group = "test"

    val brokerList = "datanode-227:9092"

    //准备kafka的参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokerList,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    kafkaStream.foreachRDD(rdd =>{

      if(!rdd.isEmpty()) {

        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        val lines = rdd.map(_.value())

        lines.persist();

        OrderUtils.calculateTotal(lines)

        OrderUtils.calculateTotalBySort(lines)

        OrderUtils.calculateTotalByProvince(lines, broadcastRef)

        kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

      }
    })

    ssc.start()

    ssc.awaitTermination()
  }

}
