package com.dicon.spark.core.DistributedTest

class DTTask extends Serializable {

  val datas = List(1,2,3,4)

  val logic: (Int)=> Int = _*2

  def compute():List[Int]={
    datas.map(logic)
  }
}
