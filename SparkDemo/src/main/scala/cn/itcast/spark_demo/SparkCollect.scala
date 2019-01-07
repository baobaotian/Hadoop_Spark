package cn.itcast.spark_demo

import org.apache.spark.{SparkConf, SparkContext}

object SparkCollect {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("SparkCollet")

    val sc = new SparkContext(conf)
    val studentRdd=sc.textFile("D:\\idea_project\\Hadoop_Spark\\SparkDemo\\data\\students.txt")

    /**
      * collect直接执行
      * 出发job任务
      * 将数据拉到一个结点，若果数据量太大，会出现OOM:程序申请内存过大，虚拟机无法满足我们，然后自杀了。这个现象通常出现在大图片的APP开发，或者需要用到很多图片的时候。通俗来讲就是我们的APP需要申请一块内存来存放图片的时候，系统认为我们的程序需要的内存过大，及时系统有充分的内存，比如1G，但是系统也不会分配给我们的APP，故而抛出OOM异常，程序没有捕捉异常，故而弹窗崩溃了
      */
    val arr=studentRdd.collect()
    arr.foreach(println)


  }

}
