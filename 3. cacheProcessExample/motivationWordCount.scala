
package org.iscas

import org.apache.spark.{SparkConf, SparkContext}

//noinspection ScalaStyle
object motivationWordCount {
  def main(args: Array[String]): Unit = {
    val startTime=System.currentTimeMillis()
    val conf=new SparkConf()
    var masterUrl = ""
    var inputDataPath = ""
    if (args.length==2) {
      masterUrl = args(0)
      inputDataPath = args(1)
    }
    else{
      masterUrl = "local"
      inputDataPath = "D://firewallLog.txt"
    }
    conf.setAppName("cacheProcessExample").setMaster(masterUrl)
    val sc=new SparkContext(conf)
    //val file = sc.textFile("D:/motivation.txt")
    val file = sc.textFile(inputDataPath)
    val words =file.flatMap(x=>x.split(" "))
    val pairs=words.map(word=>(word,1)).cache()
    val rawFiltered=pairs.filter(item=>
      item._1.startsWith("i")).cache()
    val fineFiltered = rawFiltered.filter(item => item._1.startsWith("id")).cache()
    val fineResults = fineFiltered.reduceByKey(_+_)
    val rawResults=rawFiltered.reduceByKey(_+_)
    val counts=rawResults.union(fineResults)
    println("counts===="+counts.first().toString());
    pairs.unpersist()
    val endTime=System.currentTimeMillis()
    val duration=endTime-startTime
    System.out.println("Duration:"+duration)
  }
}