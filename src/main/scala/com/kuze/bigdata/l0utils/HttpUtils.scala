package com.kuze.bigdata.l0utils

import scalaj.http.Http

object HttpUtils {

  def getFileByteSize(url: String):Long = {
    var size = 0L;
    try{
      val response = Http(url).method("HEAD")
        .header("Cookie","test")
        .timeout(2000,1000)
        .asString

      val headers: Map[String, IndexedSeq[String]] = response.headers

      val maybeStrings: Option[IndexedSeq[String]] = headers.get("Content-Length")

      if(maybeStrings.isDefined){
        val value: IndexedSeq[String] = maybeStrings.get
        size = value(0).toLong
      }
    }catch{
      case e: Exception => {
        println(e)
      }
    }
    size
  }

}
