package com.dicon.spark.streaming.wordcount01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object wordcount {

  def main(args: Array[String]): Unit = {

    //创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")

    //3秒批处理一次
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //逻辑处理
    val value = ssc.socketTextStream("localhost", 9999)

    val words = value.flatMap(_.split(" "))

    val wordTogroup = words.map((_, 1))

    val wordTocout = wordTogroup.reduceByKey(_ + _)

    wordTocout.print();

    //启动采集器
    ssc.start()

    //持续监听处理
    ssc.awaitTermination()
  }
}
