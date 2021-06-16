package com.kuze.bigdata.l1rdd.rdd_demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

object ListToES {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ListToES").setMaster("local[2]")
    conf.set("es.nodes", "192.168.0.140")
    conf.set("es.port","9200")
    conf.set("es.index.auto.create", "true")
    conf.set("es.net.http.auth.user","elastic")
    conf.set("es.net.http.auth.pass","XBg7TaJntHWINmiCtath")

    val sc = new SparkContext(conf)

    val resource: RDD[String] = sc.parallelize(List("hello world","aaaa"))

    val esMap: RDD[Map[String, Any]] = resource.map(o => {
      Map("bgnts" -> 1L, "vtype" -> "123", "count" -> 123, "vsize" -> 123, "tslen" -> 60)
    })

    EsSpark.saveToEs(esMap,"equipratio/doc")
  }

}
