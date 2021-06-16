package com.kuze.bigdata.l1rdd.core_partition

import org.apache.spark.Partition

class MyPartition(numPartitions: Int) extends Partition{

  override def index: Int = numPartitions

  def hashCode(key: Any): Int = {
    val domain = key.toString
    val code = (domain.hashCode % index)
    if(code < 0) {
      code + index // 使其非负
    }else{
      code
    }
  }// 用来让Spark区分分区函数对象的Java

  override def equals(other: Any): Boolean = other match {
    case dnp: MyPartition =>
      dnp.index == numPartitions
    case _ =>
      false
  }

}