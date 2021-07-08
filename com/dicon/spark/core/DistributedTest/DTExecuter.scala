package com.dicon.spark.core.DistributedTest

import java.io.ObjectInputStream
import java.net.ServerSocket

object DTExecuter {

  def main(args: Array[String]): Unit={

    //启动服务器，接收数据
    val server = new ServerSocket(9999)

    //等待客户端的连接
    val client = server.accept()

    val in = client.getInputStream
    val objIn = new ObjectInputStream(in)

    val task = objIn.readObject().asInstanceOf[DTTask]

    val result: List[Int] = task.compute()

    println(result)

    in.close()
    client.close()
    server.close()
  }
}
