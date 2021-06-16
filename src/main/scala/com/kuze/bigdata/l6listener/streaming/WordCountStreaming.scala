package com.kuze.bigdata.l6listener.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}


object WordCountStreaming {
  def main(args: Array[String]): Unit = {

    val ssc = new StreamingContext("local[*]", "StatStreamingApp", Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.0.235:9092,192.168.0.237:9092,192.168.0.229:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "streaming_test",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //StreamingContext调用addStreamingListener方法，实现StreamingListener。
    ssc.addStreamingListener(MyStreamingListener)

    val topics = Array("test")
    val lines = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    lines.foreachRDD(rdd=>{
      val reduce=rdd.map(_.value()).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
      reduce.foreach(println)

    })

    ssc.start()
    ssc.awaitTermination()
  }
}
