package com.dicon.spark.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark01_wordcount {
  def main(args:Array[String]): Unit = {

    //建立和spark框架的连接
    val sparkconf = new SparkConf().setMaster("local").setAppName("wordcount")
    val sc = new SparkContext(sparkconf)

    //执行业务操作
    /**
     * 1.读取文件，获取一行一行的数据
     * 2.拆分成单个单词
     * 3.将相同的单词分组
     * 4.对分组后的数据进行转换
     * 5.将转换结果采集到控制台打印
     */

    //1.读取文件、获取一行一行的数据
    val lines: RDD[String] = sc.textFile("F:\\spark\\input\\spark-core\\wordcount")

    //2.拆分成单个单词
    val words = lines.flatMap(_.split(" "))

    //3.将相同的单词分组
    val wordGroup: RDD[(String,Iterable[String])] = words.groupBy(word => word)

    //4.对分组后的数据进行转换
    val wordToCount = wordGroup.map {
      case (word,list) => {
        (word, list.size)
      }
    }

    //5.将转换的结果采集到控制台打印
    val array: Array[(String,Int)] = wordToCount.collect()
    array.foreach(println)

    //关闭连接
    sc.stop()

  }
}
