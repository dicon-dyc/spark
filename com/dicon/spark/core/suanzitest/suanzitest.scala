package com.dicon.spark.core.suanzitest

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object suanzitest {
  def main(args: Array[String]): Unit={

    //建立和spark框架的连接
    val sparkconf = new SparkConf().setMaster("local").setAppName("suanzitest")
    val sc = new SparkContext(sparkconf)
    /**
     * 转化算子
     */

    //map()是一种转化算子，接收一个函数作为参数，并把这个函数应用于RDD的每个元素，
    //最后将函数的返回结果作为结果RDD中对应的元素的值。
    val maprdd: RDD[Int] = sc.parallelize(List(1,2,3,4,5,6))
    val resultmap: RDD[Int] = maprdd.map(_+1)
    //resultmap.collect().foreach(x=>print(x + " "))

    //filter(func)算子是一种转化算子，接收一个函数作为参数，对每个元素进行过滤，并返回一个新的RDD。
    val rddfilter = sc.parallelize(List(1,2,3,4,5,6))
    val resultfilter = rddfilter.filter(_>3)
    //resultfilter.collect().foreach(x=>print(x + " "))

    //flatMap(func)算子是一种转化算子，接收一个函数作为参数，返回0到多个元素，最终将返回的所有元素合并到一个RDD
    val flatMaprdd = sc.parallelize(List("hello spark","hello world"))
    val resultflatMap = flatMaprdd.flatMap(_.split(" "))
    //resultflatMap.collect().foreach(x=>print(x + " "))

    //reduceByKey(func)算子是一种转化算子，作用对象是元素为（key，value）形式（scala元组）的RDD，可以将
    //相同key的元素聚集到一起，最终把所有相同key的元素合并成为一个元素。
    val reducebykeyrdd = sc.parallelize(List(("lisi",10),("zhangsan",5),("zhangsan",5),("lisi",10)))
    val resultreducebykey: RDD[(String,Int)] = reducebykeyrdd.reduceByKey(_+_)
    //resultreducebykey.collect().foreach(x=>print(x))

    //union()算子是一种转化算子，将两个RDD合并为一个新的RDD，主要用于对不同的数据来源进行合并，两个RDD中的数据
    //类型要保持一致。
    val unionrdd1 = sc.parallelize(List(1,2,3))
    val unionrdd2 = sc.parallelize(List(4,5,6))
    val resultunion = unionrdd1.union(unionrdd2)
    //resultunion.collect().foreach(print);

    //sortBy(func)算子是一种转化算子，将RDD中的元素按照某个规则进行排序，该算子第一个参数为排序函数，第二个参数
    //是一个布尔值，指定升序（默认）或降序
    val sortbyrdd = sc.parallelize(Array(("zhangsan",2),("lisi",3),("wangsan",1)))
    val resultsortby = sortbyrdd.sortBy(_._2,false)
    resultsortby.collect().foreach(println)

    /**
     * 行动算子，spark中的转化算子并不会立即进行计算，而是在遇到行动算子时才会执行相应的语句，触发spark的任务调度。
     */

    //reduce(func)算子是一种行动算子
    sc.stop()
  }
}
