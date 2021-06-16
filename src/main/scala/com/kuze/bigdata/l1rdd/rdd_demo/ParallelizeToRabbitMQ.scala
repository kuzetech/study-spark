package com.kuze.bigdata.l1rdd.rdd_demo

import com.kuze.bigdata.l0utils.SparkContextUtils
import com.rabbitmq.client._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object ParallelizeToRabbitMQ {

  def main(args: Array[String]): Unit = {

    val host = "192.168.0.236"
    val port = 5672
    val user = "usky"
    val password = "usky@123456"

    val exchangeName = "exchangeName"
    val queueName1 = "queueName1"
    val queueName2 = "queueName2"
    val topicBindingKey = "com.*"
    val directBindingKey = "com.routingKey1"
    val routingKey1 = "com.routingKey1"
    val routingKey2 = "com.routingKey2"

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resource: RDD[(Int, String)] = sc.parallelize(
      List((1555000000,"aaaa"),(1555000001,"bbbb"),(1555000002,"cccc"),(1555000003,"dddd"))
    )

    resource.foreachPartition(partition=>{
      val factory = new ConnectionFactory()
      factory.setUsername(user)
      factory.setPassword(password)
      factory.setVirtualHost("/")
      factory.setHost(host)
      factory.setPort(port)

      /**
        * 我们完全可以直接使用 Connection 就能完成信道的工作，为什么还要引入信道呢?
        * 试想这 样一个场景， 一个应用程序中有很多个线程需要从 RabbitMQ 中消费消息，
        * 或者生产消息，那 么必然需要建立很多个 Connection，也就是许多个 TCP 连接。
        * 然而对于操作系统而言，建立和 销毁 TCP 连接是非常昂贵的开销，如果遇到使用高峰，性能瓶颈也随之显现。
        * RabbitMQ 采用 类似 NIO' (Non-blocking 1/0) 的做法，选择 TCP 连接复用，不仅可以减少性能开销，同时也 便于管理 。
        * 每个线程把持一个信道，所以信道复用了 Connection 的 TCP 连接。
        * 同时 RabbitMQ 可以确 保每个线程的私密性，就像拥有独立的连接一样。
        * 当每个信道的流量不是很大时，复用单一 的 Connection 可以在产生性能瓶颈的情况下有效地节 省 TCP 连接 资源。
        * 但是当信道本身的流量很 大时，这时候多个信道复用一个 Connection 就会产生性能瓶颈，进而使整体的流量被限制了。
        * 此时就需要开辟多个 Connection，将这些信道均摊到这些 Connection 中，
        * 至于这些相关的调优 策略需要根据业务自身的实际情况进行调节，更多内容可以参考第 9章
        */
      //创建长连接（TCP连接）
      val conn = factory.newConnection
      val conn2 = factory.newConnection
      //创建信道（也是长连接，AMQP信道）
      val channel = conn.createChannel
      val channel2 = conn.createChannel

      conn.addShutdownListener(new ShutdownListener {
        override def shutdownCompleted(cause: ShutdownSignalException): Unit = {
          cause.getCause.printStackTrace()
          if (cause.isHardError()){
            val connRef: Connection = cause.getReference().asInstanceOf[Connection]
            if (!cause.isInitiatedByApplication()){
              val reason = cause.getReason()
              reason.protocolMethodName()
            }
          }else {
            val chanelRef: Channel = cause.getReference().asInstanceOf[Channel]
          }
        }
      })

      //创建一个 type="direct"、持久化的、非自动删除的交换器
      channel.exchangeDeclare(exchangeName, "direct", true)

      /**
        * 创建一个 type="fanout"、持久化的、非自动删除的交换器
        * 它会忽略routingKey，把所有发送到该交换器的消息路由到所有与该交换器绑定的队列中
        * channel.exchangeDeclare(exchangeName, "fanout", true)
        */
      /**
        * 创建一个 type="topic"、持久化的、非自动删除的交换器
        * bindingKey模糊匹配routingKey
        * bindingKey和routingKey都需要.号间隔
        * bindingKey可以包含*（匹配单个单词），#（至少零个单词）
        * channel.exchangeDeclare(exchangeName, "topic", true)
        */
      /**
        * 创建一个 type="headers"、持久化的、非自动删除的交换器
        * 它会忽略routingKey，根据发送的消息内容中的 headers 属性进行匹配
        * headers 类型的交换器性能会很差，而且也不实用，基本上不会看到它的存在
        * channel.exchangeDeclare(exchangeName, "headers", true)
        */


      //创建一个持久化、非排他的、非自动删除的队列
      val map: java.util.Map[String, Object] = new java.util.HashMap[String, Object]()
      map.put("x-message-ttl", 6000.asInstanceOf[Object])
      channel.queueDeclare(queueName1, true, false, false, map)
      channel.queueDeclare(queueName2, true, false, false, null)

      //将交换器与队列通过路由键绑定
      channel.queueBind(queueName1, exchangeName, directBindingKey)
      channel.queueBind(queueName2, exchangeName, directBindingKey)

      channel.confirmSelect //将信道置为 publisher confirm 模式
      case class Named(bgnts: Int, content: String)
      partition.foreach(item=>{
        val message = com.codahale.jerkson.Json.generate(Named(item._1,item._2))
        val messageBodyBytes: Array[Byte] = message.getBytes
        var routingKey = routingKey2
        if(item._2.contains("a") || item._2.contains("b")){
          routingKey = routingKey1
        }
        channel.basicPublish(
          exchangeName,
          routingKey,
          new AMQP.BasicProperties().builder()
            //存入两份相同的消息（副本一份）
            .deliveryMode(2)
            .build(),
          messageBodyBytes)
        if (!channel .waitForConfirms()) {
          System.out.println("send message failed ")
          // do something else..
        }
      })

      /**
        * 先提交到MQ中，再提交到HBase中
        * 该过程都没有发生错误的情况下
        * 再确认事务，否则回滚
        * 但是该机制，非常消耗性能
        */
      /*try{
        channel.txSelect()
        channel.basicPublish(
          exchangeName,
          routingKey,
          new AMQP.BasicProperties().builder()
            //存入两份相同的消息（副本一份）
            .deliveryMode(2)
            .build(),
          messageBodyBytes)
          channel.txCommit()
      }catch{
        case e: Exception => {
          channel.txRollback()
          e.printStackTrace()
        }
      }*/

      //消费者和生产者可以共用同一个tcp连接和信道
      //val response = channel.queueDeclarePassive(queueName1)
      //println(response.getMessageCount)
      //println(response.getConsumerCount)

      channel.close
      channel2.close
      conn.close
      conn2.close
    })
  }
}
