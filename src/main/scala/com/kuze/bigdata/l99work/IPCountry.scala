package com.kuze.bigdata.l99work

import com.kuze.bigdata.l0utils.{IpUtils, SparkContextUtils}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object IPCountry {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val ipRules: Array[(Long, Long, String)] = IpUtils.readTable("/Users/baiye/Downloads/table.txt")

    val ipRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(ipRules)

    val log = sc.textFile("/Users/baiye/Downloads/ddoschanel.txt")

    val chanFilter: RDD[String] = log.filter(o=>o.contains("www.cloudtopspeed.com"))

    val cipRDD: RDD[String] = chanFilter.map(o => {
      val messageContent: String = o.substring(o.indexOf("qf_cip\":\"") + 9, o.length)
      val cip = messageContent.substring(0, messageContent.indexOf("\""))
      cip
    })

    val distinctCipRDD: RDD[String] = cipRDD.distinct()

    val countryAndOne: RDD[(String, Int)] = distinctCipRDD.map(o => {
      val ipLong: Long = IpUtils.ip2Long(o)
      var country = "未知"
      val ruleMap = ipRef.value;
      val index: Int = IpUtils.binarySearch(ruleMap, ipLong)
      if (index != -1) {
        country = ipRef.value(index)._3
      }
      (country, 1)
    })

    countryAndOne.persist()

    val total: Long = countryAndOne.count()

    println(s"总IP数量为${total}")

    val reduce: RDD[(String, Int)] = countryAndOne.reduceByKey(_ + _)

    val sort: RDD[(String, Int)] = reduce.sortBy(_._2,false)

    val result: Array[(String, Int)] = sort.collect()

    result.foreach(o=>{
      val percent = (o._2.toDouble / total * 100).formatted("%.4f")
      println(s"${o._1} 占 ${percent}%")
    })

    countryAndOne.unpersist()

  }

}
