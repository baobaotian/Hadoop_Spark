package cn.itcast.spark_demo

import org.apache.spark.{SparkConf, SparkContext}

object SparkFilter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("SparkFilter")
    val sc = new SparkContext(conf)
    val lineRDD = sc.textFile("D:\\idea_project\\Hadoop_Spark\\SparkDemo\\data\\students.txt")

    /**
      * filter 懒执行
      * 返回true保留数据
      * 返回flase过滤数据
      */
    //把"女"写在前面，方式空指针溢出
    lineRDD.filter(line => "女".equals(line.split(",")(3)))
      .foreach(println)

  }

}
