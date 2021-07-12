package com.dicon.spark.streaming.DIYreceiver

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

object DIYreceiver {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("DIYreceiver")

    val ssc = new StreamingContext(sparkconf,Seconds(3))

    val value = ssc.receiverStream(new Myreceiver)

    value.print()

    ssc.start()
    ssc.awaitTermination()
  }

  class Myreceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    private var flg = true
    override def onStart(): Unit = {

      new Thread(new Runnable {
        override def run(): Unit = {
          while (flg){

            val message = new Random().nextInt(10).toString
            store(message)
            Thread.sleep(500)
          }
        }
      }).start()
    }

    override def onStop(): Unit = {

      flg = false
    }
  }
}
