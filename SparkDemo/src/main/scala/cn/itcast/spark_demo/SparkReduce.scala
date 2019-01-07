package cn.itcast.spark_demo

import org.apache.spark.{SparkConf, SparkContext}

object SparkReduce {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setMaster("local")
      .setAppName("SparkReduce")

    val sc = new SparkContext(conf)
    val list =List(1,2,3,4,5,6,7,8)

    val RDD1 = sc.parallelize(list)
    val sum = RDD1.reduce(_ + _)
    val sum1 = RDD1.sum()

    println(sum)
    println(sum1)


  }

}
