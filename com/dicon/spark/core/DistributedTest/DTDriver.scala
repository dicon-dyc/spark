package com.dicon.spark.core.DistributedTest

import java.io.ObjectOutputStream
import java.net.Socket

object DTDriver {

  def main(args:Array[String]): Unit = {

    //连接服务器
    val client = new Socket("localhost",9999)

    val out = client.getOutputStream
    val objOut = new ObjectOutputStream(out)

    val task = new DTTask()

    objOut.writeObject(task)
    out.flush()
    out.close()
    client.close()
  }
}
