package com.kuze.bigdata.l99work

import com.kuze.bigdata.l0utils.{IpUtils, SparkContextUtils}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object IPState {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val ipRules: Array[(Long, Long, String)] = IpUtils.readTable("/Users/baiye/Downloads/table.txt")

    val ipRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(ipRules)

    val countryRules = IpUtils.readState("/Users/baiye/Downloads/country.txt")

    val countryRef = sc.broadcast(countryRules)

    val log = sc.textFile("/Users/baiye/Downloads/ddoschanel.txt")

    val chanFilter: RDD[String] = log.filter(o=>o.contains("www.cloudtopspeed.com"))

    val cipRDD: RDD[String] = chanFilter.map(o => {
      val messageContent: String = o.substring(o.indexOf("qf_cip\":\"") + 9, o.length)
      val cip = messageContent.substring(0, messageContent.indexOf("\""))
      cip
    })

    val distinctCipRDD: RDD[String] = cipRDD.distinct()

    distinctCipRDD.persist()

    val total: Long = distinctCipRDD.count()

    println(s"攻击IP总数量为${total}个")

    val countryAndOne: RDD[(String, Int)] = distinctCipRDD.map(o => {
      val ipLong: Long = IpUtils.ip2Long(o)
      var country = "未知"
      val ruleMap = ipRef.value
      val index: Int = IpUtils.binarySearch(ruleMap, ipLong)
      if (index != -1) {
        country = ipRef.value(index)._3
      }
      (country, 1)
    })

    val countryAndTotal: RDD[(String, Int)] = countryAndOne.reduceByKey(_ + _)

    val stateAndCount: RDD[(String, Int)] = countryAndTotal.map(o => {
      var state = "未知"
      val countryMap = countryRef.value
      val option: Option[String] = countryMap.get(o._1)
      if (option.isDefined) {
        state = option.get.toString
      }
      (state, o._2)
    })

    val stateAndTotal: RDD[(String, Int)] = stateAndCount.reduceByKey(_ + _)

    val sort: RDD[(String, Int)] = stateAndTotal.sortBy(_._2,false)

    val result: Array[(String, Int)] = sort.collect()

    result.foreach(o=>{
      val percent = (o._2.toDouble / total * 100).formatted("%.4f")
      println(s"${o._1} 占 ${percent}%")
    })

    distinctCipRDD.unpersist()

  }

}
