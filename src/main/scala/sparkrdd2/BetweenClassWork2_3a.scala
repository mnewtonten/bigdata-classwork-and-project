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

object BetweenClassWork2_3 extends JFXApp {

  val conf = new SparkConf().setAppName("Hello Work").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val lines2016 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/2016.csv")
  val lines1897 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/1897.csv")
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
  
  val avg2016 = calcYear(lines2016)
  val avg1897 = calcYear(lines1897) 
   
  //joining the two RDDs to make (ID, (AVG_TEMP, (LATITUDE, LONGITUDE)))
  //val avgTempData = avgTempsByStation.join(stationLoc)


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
  System.out.println("Avg for all of 2016 was " +avg2016+" avg for all of 1897 was "+avg1897+"\nThe average of the two of them together is "+((avg2016+avg1897)/2))
}
