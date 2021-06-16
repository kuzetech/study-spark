package com.kuze.bigdata.l99work

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


object MySqlToHDFS_CV {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder
      .appName("MySqlToHDFS_CV")
      .master("local[4]")
      .getOrCreate

    val jdbcDF: DataFrame = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://192.168.0.240:3306/rdc")
      .option("dbtable", "(select * from dwd_reg where DATE_SUB(CURDATE(), INTERVAL 3 DAY) <= log_date")
      .option("user", "usky")
      .option("password", "usky@123456")
      .load()

    /*val value: RDD[String] = jdbcDF.map(o=>{
      var province = "未知"
      if(o.getString(2)!=null){
        province = o.getString(2).substring(1,o.getString(2).length-1)
      }
      var city = "未知"
      if(o.getString(3)!=null){
        city = o.getString(3).substring(1,o.getString(3).length-1)
      }
      o.getLong(0) + "|" + o.getLong(1) + "|" + province + "|" + city
    }).rdd*/

    //val value: RDD[String] = jdbcDF.rdd.map(o=>o.getLong(0) + "|" + o.getLong(1) + "|" + o.getString(2))

    //value.saveAsTextFile("/Users/baiye/Downloads/table")

    //采集下来之后，重命名文件，上传到HDFS指定位置
    /*
    scp -P 57892 rules.txt root@103.118.36.196:~/
    YTkj123!@#
    hdfs dfs -rm -r -f /ipTable/rules.txt
    hdfs dfs -put -f ~/rules.txt /ipTable
    hdfs dfs -cat /ipTable/rules.txt
    */

  }

}
