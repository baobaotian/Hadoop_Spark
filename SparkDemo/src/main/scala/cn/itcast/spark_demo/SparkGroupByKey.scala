package cn.itcast.spark_demo

import org.apache.spark.{SparkConf, SparkContext}

object SparkGroupByKey {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setMaster("local")
      .setAppName("SparkGroupByKey")
    val sc = new SparkContext(conf)
    val lineRDD = sc.textFile("D:\\idea_project\\Hadoop_Spark\\SparkDemo\\data\\students.txt")

    /**
      * 统计每个班的人数
      */
    val tupleRDD=lineRDD
      .map(line => line.split(",")(4))
      .map(clazz => (clazz,1))

    /**
      * groupByKey 懒执行
      * 只能对k-v格式的RDD上
      * 默认是hash分区
      */
    //groupByKey知识按照key分组，需要后序map处理
    /**
      * 输出：
      * (理科二班,CompactBuffer(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1))
      * (文科三班,CompactBuffer(1, 1, 1, 1,  1, 1, 1, 1, 1, 1, 1, 1, 1))
      * (理科四班,CompactBuffer(1, 1,1, 1))
      * (理科一班,CompactBuffer(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1))
      * (文科五班,CompactBuffer(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1))
      * (文科一班,CompactBuffer(1, 1, 1, 1, 1, 1))
      * (文科四班,CompactBuffer(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1))
      * (理科六班,CompactBuffer(1, 1, , 1, 1, 1, 1, 1, 1))
      * (理科三班,CompactBuffer(1, 1))
      * (文科六班,CompactBuffer(1, 1,1, 1 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1))
      * (理科五班,CompactBuffer(1, 1, 1, 1))
      * (文科二班,CompactBuffer(1, 1, 1, 1, 1, 1))
      *
      */
    tupleRDD.groupByKey()
      //.map(x => (x._1,x._2.sum))
      .foreach(println)

    /**
      * map()处理后
      * 输出：
      * (理科二班,79)
      * (文科三班,94)
      * (理科四班,91)
      * (理科一班,78)
      * (文科五班,84)
      * (文科一班,72)
      * (文科四班,81)
      * (理科六班,92)
      * (理科三班,68)
      * (文科六班,104)
      * (理科五班,70)
      * (文科二班,87)
      */
    tupleRDD.groupByKey()
      .map(x => (x._1,x._2.sum))
      .foreach(println)

    /**
      * reduceByKey
      */
    /**
      * 输出：
      * (理科二班,79)
      * (文科三班,94)
      * (理科四班,91)
      * (理科一班,78)
      * (文科五班,84)
      * (文科一班,72)
      * (文科四班,81)
      * (理科六班,92)
      * (理科三班,68)
      * (文科六班,104)
      * (理科五班,70)
      * (文科二班,87)
      */
    tupleRDD.reduceByKey((x,y) => x + y)
    tupleRDD.reduceByKey(_ + _).foreach(println)
  }

}
