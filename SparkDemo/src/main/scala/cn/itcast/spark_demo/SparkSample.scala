package cn.itcast.spark_demo

import org.apache.spark.{SparkConf, SparkContext}

object SparkSample {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setMaster("local")
      .setAppName("SparkSample")

    val sc = new SparkContext(conf)
    val lineRDD=sc.textFile("D:\\idea_project\\Hadoop_Spark\\SparkDemo\\data\\students.txt")


    /**
      * sample 懒执行
      * withRepalcement 是否放回
      * fraction  抽样比例
      */

    val sampleRDD = lineRDD.sample(true,0.1D)

    println(sampleRDD.count())
  }

}
