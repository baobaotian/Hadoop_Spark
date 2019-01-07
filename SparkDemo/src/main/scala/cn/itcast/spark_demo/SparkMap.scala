package cn.itcast.spark_demo

import org.apache.spark.{SparkConf, SparkContext}

object SparkMap {
  def main(args: Array[String]): Unit = {
    //rdd来源两个：一个scala的数组，hdfs的文件
    val list = List(1, 2, 3, 4, 5, 6)
    val conf = new SparkConf().setMaster("local").setAppName("SparkMap")
    val sc = new SparkContext(conf)
    /**
      *  sc.textFile("")
      * spark 本身没有读取文件的方法，用的还是mapreduce读取文件的方法
      *
      * local模式，是读取本地文件
      * yarn模式，读取hdfs
      */
    //把scala集合序列化成一个RDD
    val listRdd = sc.parallelize(list)

    /**
      * map  传入一个对象只返回一个对象
      * 懒执行  lazzy
      */
    val RDD2 = listRdd.map(x => x * x)
    RDD2.foreach(println)


    val linesRDD = sc.textFile("D:\\idea_project\\Hadoop_Spark\\SparkDemo\\data\\students.txt")
    linesRDD.map(line => line.split(","))
      .map(arr => Student(arr(0), arr(1), arr(2).toInt, arr(3), arr(4)))
      .foreach(s => println(s.name + " " + s.clazz))


  }

}

case class Student(id: String, name: String, age: Int, gender: String, clazz: String)
