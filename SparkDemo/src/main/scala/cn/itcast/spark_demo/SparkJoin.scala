package cn.itcast.spark_demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zbf
  *  join是按照key值join在一起的，需要k-v格式的RDD
  *  主要区分x._1._2这类格式
  */
object SparkJoin {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setMaster("local")
      .setAppName("SparkJoin")
    val sc = new SparkContext(conf)
    val studentsRDD = sc.textFile("D:\\idea_project\\Hadoop_Spark\\SparkDemo\\data\\students.txt")
    val scoresRDD=sc.textFile("D:\\idea_project\\Hadoop_Spark\\SparkDemo\\data\\score.txt")

    val studentKVRDD = studentsRDD.map(lines => (lines.split(",")(0),lines))
    val scoreKVRDD = scoresRDD.map(lines => (lines.split(",")(0),lines))

    val joinRDD = studentKVRDD.join(scoreKVRDD)
    joinRDD.take(20)

    /**
      * (1500100668,(1500100668,侯从寒,23,女,文科一班,1500100668,1000001,146))
      * (1500100668,(1500100668,侯从寒,23,女,文科一班,1500100668,1000002,49))
      * (1500100668,(1500100668,侯从寒,23,女,文科一班,1500100668,1000003,53))
      * (1500100668,(1500100668,侯从寒,23,女,文科一班,1500100668,1000004,78))
      * (1500100668,(1500100668,侯从寒,23,女,文科一班,1500100668,1000005,25))
      * (1500100668,(1500100668,侯从寒,23,女,文科一班,1500100668,1000006,22))
      */
    joinRDD.foreach(println)

    /**
      * 1500100668,1000001,146
      * 1500100668,1000002,49
      * 1500100668,1000003,53
      * 1500100668,1000004,78
      * 1500100668,1000005,25
      * 1500100668,1000006,22
      */
    joinRDD.take(20)
    joinRDD.map(x => x._2._2).foreach(println)
    joinRDD.take(20)
    joinRDD.map(x => x._2._1).foreach(println)

    //一个item:
    //(1500100329,(1500100329,秦又绿,23,女,文科四班,1500100329,1000002,74))
//    joinRDD.map(x => {
//      val id = x._1
//      val stuInfo = x._2._1
//      val scoInfo = x._2._2
//      val scoArr = scoInfo.split(",")
//      stuInfo + "," + scoArr(1) + "," + scoArr(2)
//    })
  }

}
