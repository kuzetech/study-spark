package com.kuze.bigdata.l3dataframe

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

/**
 * App
 *
 * @author baiye
 * @date 2020/9/18 上午11:56
 */
class App {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("test")
      .master("local")
      .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", "5")

    val scheme: StructType = StructType(Array(
      StructField("dest_country", StringType, true),
      StructField("origin_country", StringType, true),
      StructField("count", LongType, true)
    ))

    val df: DataFrame = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .schema(scheme)
      .csv("path")

    df.as()

    df.createOrReplaceTempView("view_name")

    val sql: DataFrame = spark.sql(
      """
      select country, count(1)
      from view_name
      group by country
    """)

    val fun: DataFrame = df.groupBy("country").count()

    df.groupBy("country")
      .sum("count")
      .withColumnRenamed("sum(count)", "dest_total")
      .sort(functions.desc("dest_total"))
      .limit(5)
      .show()




  }

}
