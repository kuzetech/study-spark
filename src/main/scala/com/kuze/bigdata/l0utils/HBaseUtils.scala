package com.kuze.bigdata.l0utils

import java.util.UUID

object HBaseUtils {

  def getUUID: String = {
    val s: String = UUID.randomUUID.toString
    s.substring(0, 8) + s.substring(9, 13) + s.substring(14, 18) + s.substring(19, 23) + s.substring(24)
  }

  def getRowKey(record :((Long, String, String, Int, Int))): (Array[Byte], String)  = {
    //val index: Int = (Math.random * 99).toInt
    //val formatIndex: String = String.format("%03d", index)
    val formatIndex: String = "099"
    val time = (record._1 /1000).toInt.toString
    val pid = record._4.toString
    val cid = record._5.toString
    val uuid = getUUID
    val len = time.getBytes.length+pid.getBytes.length+cid.getBytes.length
    val rowKeysBytes = new Array[Byte](formatIndex.length+len+8)
    var position = 0
    System.arraycopy(formatIndex.getBytes, 0, rowKeysBytes, 0, formatIndex.length)
    position = position + formatIndex.length
    System.arraycopy(time.getBytes, 0, rowKeysBytes, position, time.getBytes.length)
    position = position + time.getBytes.length
    System.arraycopy(pid.getBytes, 0, rowKeysBytes, position, pid.getBytes.length)
    position = position + pid.getBytes.length
    System.arraycopy(cid.getBytes, 0, rowKeysBytes, position, cid.getBytes.length)
    position = position + cid.getBytes.length
    System.arraycopy(uuid.getBytes, 0, rowKeysBytes, position, 8)
    (rowKeysBytes,uuid)
  }

  def getTestRowKey(key :String): (Array[Byte], String)  = {
    val rowKeysBytes = new Array[Byte](key.length+8)
    var position = 0
    System.arraycopy(key.getBytes, 0, rowKeysBytes, 0, key.length)
    position = position + key.length
    val uuid = getUUID
    System.arraycopy(uuid.getBytes, 0, rowKeysBytes, position, 8)
    (rowKeysBytes,uuid)
  }

}
