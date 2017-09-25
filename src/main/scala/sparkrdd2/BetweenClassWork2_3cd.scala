package sparkrdd2

import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scalafx.application.JFXApp
import org.apache.spark._
import swiftvis2.plotting._
import swiftvis2.plotting.ColorGradient
import swiftvis2.plotting.renderer.FXRenderer

object BetweenClassWork2_3cd extends JFXApp {

  val conf = new SparkConf().setAppName("Hello Work").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val lines1897 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/1897.csv")
  val lines1907 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/1907.csv")
  val lines1917 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/1917.csv")
  val lines1927 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/1927.csv")
  val lines1937 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/1937.csv")
  val lines1947 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/1947.csv")
  val lines1957 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/1957.csv")
  val lines1967 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/1967.csv")
  val lines1977 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/1977.csv")
  val lines1987 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/1987.csv")
  val lines1997 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/1997.csv")
  val lines2007 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/2007.csv")
  val lines2016 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/2016.csv")
  val stationsLines = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/ghcnd-stations.txt")





  
  //Case Class for Stations
  case class Stations(id: String,lat: Double, long: Double, state: String, name: String)
  val stations = stationsLines.map{line =>
    Stations(line.substring(0,11),line.substring(12,20).toDouble, line.substring(21,30).toDouble,line.substring(38,40),line.substring(41,71))
  }


  
  case class SData(id: String, date: Int, elem: String, value: Double)
 


  def calcYear(yearLines: RDD[String]):  Double = {
   
  //Case Class for 2017 - and sData, RDD of Case Class SData
  val sData = yearLines.map {line =>
    val p = line.split(",")
    SData(p(0).toString, p(1).toInt, p(2).toString, p(3).toDouble)
  }
  
  //making a pair RDD of (ID, AVG_TEMP)  
  val valuePair = sData.map(x => (x.id) -> (x.elem,(((x.value/10)*(9.0/5.0))+32)))
  val temps = valuePair.filter(x => (x._2._1 == "TMAX" || x._2._1 == "TMIN")).map(x => (x._1) -> (x._2._2))
  val tempsByStation = temps.groupByKey()
  val avgTempsByStation = tempsByStation.map(x => (x._1) -> (x._2.sum/x._2.size))
  val avgTemps = avgTempsByStation.map(_._2)
  val yearAverage = (avgTemps.sum/avgTemps.count)
  return yearAverage
  }
  
  
  
  val avg1897 = calcYear(lines1897) 
  val avg1907 = calcYear(lines1907) 
  val avg1917 = calcYear(lines1917) 
  val avg1927 = calcYear(lines1927) 
  val avg1937 = calcYear(lines1937) 
  val avg1947 = calcYear(lines1947) 
  val avg1957 = calcYear(lines1957) 
  val avg1967 = calcYear(lines1967) 
  val avg1977 = calcYear(lines1977) 
  val avg1987 = calcYear(lines1987) 
  val avg1997 = calcYear(lines1997) 
  val avg2007 = calcYear(lines2007) 
  val avg2016 = calcYear(lines2016)
   
  val xPnt = Array(1897, 1907, 1917, 1927, 1937, 1947, 1957, 1967, 1977, 1987, 1997, 2007, 2016)
  val yPnt = Array(avg1897, avg1907, avg1917, avg1927, avg1937, avg1947, avg1957, avg1967,
    avg1977, avg1987, avg1997, avg2007, avg2016)
  val plot = Plot.scatterPlot(xPnt, yPnt, "Average Temperatures for All Stations", "Year", "Temperature")
  FXRenderer(plot)

  // val testColl = avgTempData.collect
  /*
  val xPnt = testColl.map(_._2._2._2)
  val yPnt = testColl.map(_._2._2._1)
  val color = testColl.map(_._2._1)
  val cg = ColorGradient(0.0 -> BlueARGB, 50.0 -> GreenARGB, 100.0 -> RedARGB)
  //val plot = Plot.scatterPlot(xPnt,yPnt, "Temperatures","Latitude","Longitude",color.map(cg))
  val plot = Plot.scatterPlot(xPnt,yPnt, "Temperatures","Latitude","Longitude",5,color.map(cg))
  FXRenderer(plot)
  */

  //avgTempsByStation.take(5) foreach println
}
