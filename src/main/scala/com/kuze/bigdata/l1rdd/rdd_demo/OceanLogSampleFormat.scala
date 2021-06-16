package com.kuze.bigdata.l1rdd.rdd_demo

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object OceanLogSampleFormat {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val resources: RDD[String] = sc.textFile("/Users/baiye/Downloads/untitled.txt")

    val filters: RDD[String] = resources.filter(o=>o.contains("#FlowDetected-") && o.contains("="))

    val substrings: RDD[String] = filters.map(item => item.substring(item.indexOf("#FlowDetected-") + 14, item.length-1))

    val tuple: RDD[(String, String)] = substrings.map(o=>(o.split("=")(0),o))

    val group: RDD[(String, Iterable[String])] = tuple.groupByKey()

    val result: Array[(String, Iterable[String])] = group.collect()

    result.foreach(o=>{
      if(!o._1.equals("statflowcachezumaxbwout")
        && !o._1.equals("wslog_704")
        && !o._1.equals("statflowchanareahit")
        && !o._1.equals("statflowcachezuflowin")
        && !o._1.equals("statflowuserareaflow")
        && !o._1.equals("statflowcachezuflowout")
        && !o._1.equals("statflowchanareaorihit")
        && !o._1.equals("statflowcachezumaxbwin")
        && !o._1.equals("statflowchanareaoriflow")
        && !o._1.equals("statflowuserareahit")
        && !o._1.equals("statflowuserareaorihit")
        && !o._1.equals("statflowcachezunl")
        && !o._1.equals("statflowchanareaflow")
        && !o._1.equals("statflowuserareaoriflow")
      ){
        o._2.foreach(println(_))
      }

    })
  }

}
