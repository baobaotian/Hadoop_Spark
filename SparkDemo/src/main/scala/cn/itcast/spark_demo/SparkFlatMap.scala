package cn.itcast.spark_demo

import org.apache.spark.{SparkConf, SparkContext}

object SparkFlatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("SparkFlatMap")
    val sc = new SparkContext(conf)
    val list=List("java is comparable","c++ is powerfule","python is bycyle")
    val RDD1 = sc.parallelize(list)

    /**
      * flatmap  懒执行
      * 传入一个对象，返回一个序列
      *  1.map操作
      *  2.对返回的序列做扁平化操作
      */
    RDD1.flatMap(line => line.split(" "))
      .foreach(println)

  }

}
