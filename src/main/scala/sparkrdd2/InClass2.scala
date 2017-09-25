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

object InClass2 extends JFXApp {

  val conf = new SparkConf().setAppName("Hello Work").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val lines2017 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/2017.csv")
  val lines1897 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/1897.csv")
  val stationsLines = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/ghcnd-stations.txt")
  
  //Case Class for Stations
  case class Stations(id: String,lat: Double, long: Double, state: String, name: String)
  val stations = stationsLines.map{line =>
    Stations(line.substring(0,11),line.substring(12,20).toDouble, line.substring(21,30).toDouble,line.substring(38,40),line.substring(41,71))
  }


  val stationList = stations.map(x => (x.id) -> (x.name)).groupByKey
  
  
  case class SData(id: String, date: String, elem: String, value: Double)
 
  
  def setUp(yearLines: RDD[String]):  RDD[(String, Double)] = {
   
  //Case Class for 2017 - and sData, RDD of Case Class SData
  val sData = yearLines.map {line =>
    val p = line.split(",")
    SData(p(0).toString, p(1).toString, p(2).toString, p(3).toDouble)
  }
  
  //making a pair RDD of (ID, AVG_TEMP) 
  //val yearValue = sData.take(1)(2).date.substring(0,4).toDouble 
  //val yearValue = year
  val valuePair = sData.map(x => (x.id) -> ((((x.value/10)*(9.0/5.0))+32)))
  return valuePair
  }
  //val highTemps = valuePair.filter(x => (x._2._1 == "TMAX")).map(_._2._2)
  //val lowTemps = valuePair.filter(x => (x._2._1 == "TMIN")).map(_._2._2)
  //val highTempsByStation = highTemps.groupByKey()
  //val lowTempsByStation = lowTemps.groupByKey()

  //val stdHigh = highTemps.stdev()
  //val stdLow = lowTemps.stdev()
  //val maxTempsByStation = highTempsByStation.filter(_._2.size >= 2).map(x => (x._1) -> (x._2.fold(0.0)((acc,report) => {if (acc < report) report else acc})))
  //val minTempsByStation = lowTempsByStation.filter(_._2.size >= 2).map(x => (x._1) -> (x._2.fold(100.0)((acc,report) => {if (acc > report) report else acc})))
  //val avgTempsByStation2 = avgTempsByStation.map(x => ((x._1),(x._2,yearValue)))

  //val differenceByStation = maxTempsByStation.join(minTempsByStation).map(x => (x._1, x._2._1 - x._2._2))
  
   
  //val maxTempDiff = differenceByStation.fold(("id",0.0))((acc, report) => {
    //if(acc._2 < report._2) report else acc})

  //val avgTemps = avgTempsByStation.map(_._2)
  //val yearAverage = (avgTemps.sum/avgTemps.count)
  //return avgTempsByStation
  //}
  
  val avges2016 = setUp(lines2017)
  val avges1897 = setUp(lines1897)
  
  val stationTotal = stationList.count
  
  val sub = stationList.subtractByKey(avges2016).union(
    stationList.subtractByKey(avges1897)).groupByKey

  val stationReports = stationTotal - sub.count

  //val subtractions = sub.map(x => (x._1,"lolwhocares"))
    
/*
  def findAvg(yearLines: RDD[(String, Double)], subtract: RDD[(String,String)]): Double = {
  val finalList = yearLines.subtractByKey(subtractions)
  val avgTemps = finalList.map(_._2)
  val yearAverage = (avgTemps.sum/avgTemps.count)
  return yearAverage
  }
   
  val avg2016 = findAvg(avges2016,subtractions)
  val avg1897 = findAvg(avges1897,subtractions)
*/ 
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

  //differenceByStation.take(5) foreach println
  
  System.out.println(stationReports)
  //System.out.println("Avg for all of 2016 was " +avg2016+" avg for all of 1897 was "+avg1897+"\nThe average of the two of them together is "+((avg2016+avg1897)/2))
}
