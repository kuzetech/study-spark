package com.kuze.bigdata.l1rdd.rdd_demo

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object OceanLogAPPFormat {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resources: RDD[String] = sc.textFile("/Users/baiye/Downloads/untitled.txt")

    val filters: RDD[String] = resources.filter(_.contains("#FlowDetected-"))

    val substrings: RDD[String] = filters.map(item => item.substring(item.indexOf("#FlowDetected-") + 14, item.length-1))

    substrings.persist()

    val cacheIntegritys: RDD[String] = substrings.filter(_.startsWith("wzdcurts"))

    val imap: RDD[(Int, Int, String, Int, Int)] = cacheIntegritys.mapPartitions(partitions => {
      var result = new ArrayBuffer[(Int, Int, String, Int, Int)]()
      partitions.foreach(item => {
        val array: Array[String] = item.split("[|]")
        val wzdcurts = array(0).substring(array(0).indexOf(">")+1, array(0).length).toInt
        val wzdmints = array(4).substring(array(4).indexOf(">")+1, array(4).length).toInt
        val cachezu = array(3).substring(array(3).indexOf(">")+1, array(3).length)
        val wzdtnum = array(1).substring(array(1).indexOf(">")+1, array(1).length).toInt
        val wzdcnum = array(2).substring(array(2).indexOf(">")+1, array(2).length).toInt
        result.+=((wzdcurts, wzdmints, cachezu, wzdtnum, wzdcnum))
      })
      result.iterator
    })

    val isort: RDD[(Int, Int, String, Int, Int)] = imap.sortBy(_._1)

    val iresult: Array[(Int, Int, String, Int, Int)] = isort.collect()

    iresult.foreach(println(_))

    val flowcachegs: RDD[String] = substrings.filter(_.startsWith("qf_vtype"))

    val cmap: RDD[(Int, String, Int, Int, Int, Int)] = flowcachegs.mapPartitions(partitions => {
      var result = new ArrayBuffer[(Int, String, Int, Int, Int,Int)]()
      partitions.foreach(item => {
        val array: Array[String] = item.split("[|]")
        val bgnts = array(5).substring(array(5).indexOf(">")+1, array(5).length).toInt
        val cachezu = array(4).substring(array(4).indexOf(">")+1, array(4).length)
        val qf_vtype = array(0).substring(array(0).indexOf(">")+1, array(0).length).toInt
        val qf_vsize = array(2).substring(array(2).indexOf(">")+1, array(2).length).toInt
        val hit = array(3).substring(array(3).indexOf(">")+1, array(3).length).toInt
        val tslen = array(1).substring(array(1).indexOf(">")+1, array(1).length).toInt
        result.+=((bgnts, cachezu, qf_vtype, qf_vsize, hit, tslen))
      })
      result.iterator
    })

    val csort: RDD[(Int, String, Int, Int, Int, Int)] = cmap.sortBy(_._1)

    val cresult: Array[(Int, String, Int, Int, Int, Int)] = csort.collect()

    cresult.foreach(println(_))
  }

}
