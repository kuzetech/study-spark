package com.kuze.bigdata.l0utils

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

object OrderUtils {

  def calculateTotal(lines: RDD[String])  = {

    val priceRDD: RDD[Double] = lines.map(line => {
      val array: Array[String] = line.split(" ")
      array(4).toDouble
    })

    val total: Double = priceRDD.reduce(_+_)

    val conn: Jedis = JedisUtils.getConnection()

    conn.incrByFloat("order-total",total)

    conn.close()

  }

  def calculateTotalBySort(lines: RDD[String]) = {

    val sortAndPrice: RDD[(String, Double)] = lines.map(line => {
      val array: Array[String] = line.split(" ")
      (array(2), array(4).toDouble)
    })

    val reduced: RDD[(String, Double)] = sortAndPrice.reduceByKey(_+_)

    reduced.foreachPartition(partial =>{

      val conn: Jedis = JedisUtils.getConnection()

      partial.foreach(total =>{

        conn.incrByFloat(total._1,total._2)

      })

      conn.close()
    })

  }

  def calculateTotalByProvince(lines: RDD[String],broadcastRef: Broadcast[Array[(Long, Long, String)]]) = {

    val provinceAndPrice: RDD[(String, Double)] = lines.map(line => {
      val array: Array[String] = line.split(" ")
      val ip: String = array(1)
      val price: Double = array(4).toDouble
      val ipLong: Long = IpUtils.ip2Long(ip)
      var province = "未知"
      val ruleMap = broadcastRef.value;
      val index: Int = IpUtils.binarySearch(ruleMap, ipLong)
      if (index != -1) {
        province = broadcastRef.value(index)._3
      }
      (province, price)
    })

    val reduced: RDD[(String, Double)] = provinceAndPrice.reduceByKey(_+_)

    reduced.foreachPartition(partial =>{

      val conn: Jedis = JedisUtils.getConnection()

      partial.foreach(total =>{

        conn.incrByFloat(total._1,total._2)

      })

      conn.close()
    })

  }

}
