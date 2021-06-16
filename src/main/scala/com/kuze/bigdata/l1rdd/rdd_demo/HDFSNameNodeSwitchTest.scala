package com.kuze.bigdata.l1rdd.rdd_demo

import com.kuze.bigdata.l0utils.SparkContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  *
  * HDFS开启高可用的时候，会同时拥有多个NameNode
  * 虽然填写正在启用的NameNode能够正常使用
  * 但如果发生错误导致NameNode进行切换时，就不能正常获取到服务
  *
  * 解决办法：
  * HDFS开启高可用之后会生成HDFS集群的nameservice，使用这个nameservice进行访问，就会自动切换namenode
  * 要使用nameservice需要在配置spark读取HDFS的配置
  * 可以在CDH-Spark配置页面寻找spark2-conf/spark-env.sh，输入export HADOOP_CONF_DIR=/etc/hadoop/conf
  *
  */
object HDFSNameNodeSwitchTest {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(SparkContextUtils.getDefaultSparkConf())

    val rulesLines: RDD[String] = sc.textFile("hdfs://MyHdfsCluster:8020/ipTable/rules.txt")

    val ipRulesRDD: RDD[(Long, Long, String, String)] = rulesLines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(0).toLong
      val endNum = fields(1).toLong
      var province = ""
      if(fields.length>2){
        province = fields(2)
      }
      var city = ""
      if(fields.length>3){
        city = fields(3)
      }
      (startNum, endNum, province,city)
    })

    val ipRulesSortRdd: RDD[(Long, Long, String, String)] = ipRulesRDD.sortBy(_._1)

    val rulesInDriver: Array[(Long, Long, String, String)] = ipRulesSortRdd.collect()

    println(rulesInDriver.toBuffer)

  }
}
