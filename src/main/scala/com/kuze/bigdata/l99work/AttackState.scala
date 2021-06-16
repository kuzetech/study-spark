package com.kuze.bigdata.l99work

import com.alibaba.fastjson.{JSON, TypeReference}
import com.kuze.bigdata.l0utils.{IpUtils, SparkContextUtils}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object AttackState {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val ipRules: Array[(Long, Long, String)] = IpUtils.readTable("/Users/baiye/Downloads/table.txt")

    val ipRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(ipRules)

    val countryRules = IpUtils.readState("/Users/baiye/Downloads/country.txt")

    val countryRef = sc.broadcast(countryRules)

    val log = sc.textFile("/Users/baiye/Downloads/ddoschanel.txt")

    val chanFilter: RDD[String] = log.filter(o=>o.contains("www.cloudtopspeed.com"))

    val cipAndCount: RDD[(String, Int)] = chanFilter.map(o => {
      val ddosChanel: DDosChanel = JSON.parseObject(o, new TypeReference[DDosChanel]() {});
      (ddosChanel.getQf_cip, ddosChanel.getCount)
    })

    val cipAndTotal: RDD[(String, Int)] = cipAndCount.reduceByKey(_ + _)

    val countryAndCount: RDD[(String, Int)] = cipAndTotal.map(o => {
      val ipLong: Long = IpUtils.ip2Long(o._1)
      var country = "未知"
      val ruleMap = ipRef.value;
      val index: Int = IpUtils.binarySearch(ruleMap, ipLong)
      if (index != -1) {
        country = ipRef.value(index)._3
      }
      (country, o._2)
    })

    val countryAndTotal: RDD[(String, Int)] = countryAndCount.reduceByKey(_ + _)

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

    var total = 0L
    result.foreach(o=>{
      total = total + o._2
    })

    println(s"总攻击次数为${total}次")

    result.foreach(o=>{
      val percent = (o._2.toDouble / total * 100).formatted("%.4f")
      println(s"${o._1} 占 ${percent}%")
    })

  }

}
