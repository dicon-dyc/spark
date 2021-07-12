package com.dicon.spark.streaming.project01.producer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.util.Random

object producer {

  def main(args: Array[String]): Unit = {

    /**
     * 生成模拟数据
     * 时间戳，省份，城市，用户，广告
     * application=》kafka=》sparkstreaming=》mysql
     */

    val prop = new Properties()

    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.146.133:9092,192.168.146.134:9092,192.168.146.135:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")


    val producer = new KafkaProducer[String,String](prop)

    while (true){
      mockdata().foreach(
        data => {

          //向kafka中生产数据
          val record = new ProducerRecord[String,String]("sparktest",data)
          producer.send(record)
        }
      )

      Thread.sleep(2000)
    }


  }

  def mockdata()={

    val list = ListBuffer[String]()
    val arealist = ListBuffer[String]("华东","华中","华南","华北")
    val citylist = ListBuffer[String]("北京","上海","广东","深圳")

    for(i <- 1 to 30){

      val area = arealist(new Random().nextInt(4))
      val city = arealist(new Random().nextInt(4))
      var userid = new Random().nextInt(6)
      var adid = new Random().nextInt(6)

      list.append(s"${System.currentTimeMillis()} ${area} ${city} ${userid} ${adid}")
    }
    list
  }
}
