package cn.itcast.spark_demo

import org.apache.spark.{SparkConf, SparkContext}

object SparkMap {
  def main(args: Array[String]): Unit = {
    val list = List(1,2,3,4,5,6)
    val conf = new SparkConf().setMaster("local").setAppName("SparkMap")
    val sc = new SparkContext(conf)



  }

}
