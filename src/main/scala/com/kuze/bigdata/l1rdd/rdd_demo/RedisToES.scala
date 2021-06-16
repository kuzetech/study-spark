package com.kuze.bigdata.l1rdd.rdd_demo

import com.redislabs.provider.redis._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

object RedisToES {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RedisToES").setMaster("local[4]")
    conf.set("redis.host", "192.168.0.228")
    conf.set("redis.port", "6379")
    conf.set("redis.auth","123")
    conf.set("redis.timeout","10000")
    conf.set("es.nodes", "datanode-228")
    conf.set("es.port","9200")
    conf.set("es.index.auto.create", "true")

    val sc = new SparkContext(conf)

    //模糊搜索key
    //redis中的数据都是（word,count）
    val redisSet: RDD[(String, String)] = sc.fromRedisKV("*")

    val wordAndCount: RDD[(String, Int)] = redisSet.map(o=>(o._1,o._2.toInt))

    val reduce: RDD[(String, Int)] = wordAndCount.reduceByKey(_ + _)

    val esMap: RDD[Map[String, Any]] = reduce.map(o => {
      Map("word" -> o._1, "count" -> o._2)
    })

    EsSpark.saveToEs(esMap,"test/docs")
  }

}
