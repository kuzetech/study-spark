package com.kuze.bigdata.l6listener.spark

import org.apache.spark.{SparkConf, SparkContext}


object WordCount {
  def main(args:Array[String]): Unit ={

    val conf = new SparkConf()
      .setAppName("WordCount").setMaster("local[*]");
    val sc = new SparkContext(conf)
    sc.addSparkListener(SparkListener)

    val lines = sc.textFile("file:/D:/test.txt");
    val words = lines.flatMap { line => line.split(" ")}
    val pairs = words.map {word => (word, 1)}
    val wordCount = pairs.reduceByKey(_ + _)
    wordCount.foreach(wordCount => println(wordCount._1 + " " + wordCount._2))
  }
}
