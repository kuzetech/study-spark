package com.kuze.bigdata.l1rdd.streaming_demo

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.codahale.jerkson.Json
import com.kuze.bigdata.l1rdd.streaming_demo.domain.WordCountDomain
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.elasticsearch.spark.rdd.EsSpark

object KafkaDirectWordCountToElasticSearchDemo {

  def main(args: Array[String]): Unit = {

    //创建SparkConf
    val conf = new SparkConf().setAppName("KafkaDirectWordCountToElasticSearchDemo").setMaster("local[4]")
    //配置ES相关参数
    /**
      * ElasticSearch 客户端程序除了Java 使用TCP的方式连接ES集群以外，其他的语言基本上都是使用的Http的方式
      * 众所周知，ES 客户端默认的TCP端口为9300，而HTTP默认端口为9200
      * elasticsearch-hadoop 使用的就是HTTP的方式连接的ES集群
      */
    conf.set("es.nodes", "datanode-228")
    conf.set("es.port","9200")
    conf.set("es.index.auto.create", "true")

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

    //从kafka接收输入流
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](Array("test"), kafkaParams)
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

      /**
        *
        * Spark写入ES可以使用elastic官方提供的Maven工具包
        * elasticsearch-hadoop，在version的选择上，可以查看该版本工具包的spark-dependency
        * 尽量与你的spark版本吻合，不然可能出现method not fund等等问题（spark版本不同scala编译版本不同所导致）
        * 该方法体里面提供三种方法：Map数据类型写入，Map数据类型带ES元数据写入，json数据类型写入（也可以写入元数据，包含在json串中即可）
        * 通过阅读底层实现可知，传输数据时直接把对象序列化成byte进行传送，因此采用json格式为最优
        * 生成json串可以通过maven引入工具jerkson_2.11
        *
        * 关于ES元数据
        * ES中元数据大体分为五中类型：
        *
        * 身份元数据
        * _index:文档所属索引 , 自动被索引，可被查询，聚合，排序使用，或者脚本里访问
        * _type：文档所属类型，自动被索引，可被查询，聚合，排序使用，或者脚本里访问
        * _id：文档的唯一标识，建索引时候传入 ，不被索引， 可通过_uid被查询，脚本里使用，不能参与聚合或排序
        * _uid：由_type和_id字段组成，自动被索引 ，可被查询，聚合，排序使用，或者脚本里访问
        *
        * 索引元数据
        * _all： 自动组合所有的字段值，以空格分割，可以指定分器词索引，但是整个值不被存储，所以此字段仅仅能被搜索，不能获取到具体的值
        * _field_names：索引了每个字段的名字，可以包含null值，可以通过exists查询或missing查询方法来校验特定的字段
        * _timestamp：可以手工指定时间戳值，也可以自动生成使用now()函数，除此之外还可以设置日期的格式化，忽略确实等功能
        * _ttl：对于一些会话数据或者验证码失效时间，一般来说是有生命周期的，在es中可以很方便的通过这个ttl来设置存活时间，比如1小时，或者10分钟，在超时过后，这个doc会被自动删除，这种方式并不适合按周或按天删除历史数据，如果是这种需求，可考虑使用索引级别的管理方式
        *
        * 文档元数据
        * _source ： 一个doc的原生的json数据，不会被索引，用于获取提取字段值 ，启动此字段，索引体积会变大，如果既想使用此字段又想兼顾索引体积，可以开启索引压缩https://www.elastic.co/guide/en/elasticsearch/reference/2.3/index-modules.html#index-codec
        * _size： 整个_source字段的字节数大小，需要单独安装一个插件才能展示，详情参见：https://www.elastic.co/guide/en/elasticsearch/plugins/5.4/mapper-size.html
        *
        * 路由元数据
        * _parent：在同一个索引中，可以通过_parent字段来给两个不同mapping type的数据建立父子关系，在查询时可以通过has_child, has_parent等查询，来聚合join数据，需要注意的是，父子type必须不能是一样的，否则会识别失败。
        * _routing： 一个doc可以被路由到指定的shard上，通过下面的规则：shard_num = hash(_routing) % num_primary_shards。默认情况下，会使用doc的_id字段来参与路由规则，如果此doc有父子关系，则会以父亲的_id作为路由规则，以确保父子数据 必须处于同一个shard上，以提高join效率
        *
        * 其他类型的元数据
        * _meta：每个mapping type可以有不同的元数据类型，我们可以存储自己定义认为的元数据中，此字段支持查询和更新
        *
        */

      //Map方式
      val mapValue: RDD[Map[String, Any]] = reduced.map(o => {
        val time = (System.currentTimeMillis() / 60000) * 60
        Map("time" -> time, "wordName" -> o._1, "wordCount" -> o._2)
      })
      EsSpark.saveToEs(mapValue,"test/docs")

      //Json形式
      case class Message(time: Long, wordName: String, wordCount: Int)
      val jsonValue: RDD[String] = reduced.map(o => {
        val time = (System.currentTimeMillis() / 60000) * 60
        Map("time" -> time, "wordName" -> o._1, "wordCount" -> o._2)
        Json.generate(Message(time,o._1,o._2))
      })
      EsSpark.saveJsonToEs(jsonValue,"test/docs")

      val aliJsonValue: RDD[String] = reduced.map(o => {
        val time = (System.currentTimeMillis() / 60000) * 60
        val obj = new WordCountDomain
        obj.setTime(time)
        obj.setWordName(o._1)
        obj.setWordCount(o._2)
        JSON.toJSONString(obj, SerializerFeature.WriteMapNullValue)
      })
      EsSpark.saveJsonToEs(aliJsonValue,"test/docs")

      //更新偏移量
      kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
