package com.kuze.bigdata.l1rdd.streaming_demo

import com.kuze.bigdata.l0utils.JedisUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import redis.clients.jedis.Jedis

object KafkaDirectWordCountToRedisDemo {
  def main(args: Array[String]): Unit = {

    //创建SparkConf
    val conf = new SparkConf().setAppName("KafkaDirectWordCountToRedisDemo").setMaster("local[4]")

    //创建SparkStreaming，并设置间隔时间
    val streamingContext = new StreamingContext(conf, Milliseconds(5000))

    //准备kafka的参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "datanode-227:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "g001",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](Array("test"), kafkaParams)
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

      reduced.foreachPartition(partition =>{
        val conn: Jedis = JedisUtils.getConnection()
        partition.foreach(total =>{
          //增加
          conn.incrByFloat(total._1,total._2)
          //一天过期
          conn.expire(total._1,60 * 60 * 24)
        })
        conn.close()
      })

      //更新偏移量
      kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
