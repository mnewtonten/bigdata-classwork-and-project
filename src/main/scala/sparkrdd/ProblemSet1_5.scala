package sparkrdd2

import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scalafx.application.JFXApp
import org.apache.spark._
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.FXRenderer

object ProblemSet1_5 extends JFXApp {

  val conf = new SparkConf().setAppName("Hello Work").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val lines2017 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/2017.csv")
  val stationsLines = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/ghcnd-stations.txt")
  
  //Case Class for Stations
  case class Stations(id: String,lat: Double, long: Double, state: String, name: String)
  val stations = stationsLines.map{line =>
    Stations(line.substring(0,11),line.substring(12,20).toDouble, line.substring(21,30).toDouble,line.substring(38,40),line.substring(41,71))
  }

  //Case Class for 2017 - and sData, RDD of Case Class SData
  case class SData(id: String, date: Int, elem: String, value: Double)
  val sData = lines2017.map {line =>
    val p = line.split(",")
    SData(p(0).toString, p(1).toInt, p(2).toString, p(3).toDouble)
  }

  val stationsPair = stations.map(x => (x.id, x.name))
  val saStations = stations.filter(x => (x.name.substring(0,11) == "SAN ANTONIO" && x.id.substring(0,2) == "US")).map(
    x=> (x.id,x.name))
  val tempData = sData.filter(x => (x.elem == "TMAX" || x.elem == "TMIN")).map(
    x=> ((x.id,x.date), (x.value)))
  val dailyIncrease = tempData.groupByKey.filter(x => x._2.size == 2).map(
    x=> (x._1._1, (x._2.toList(0) - x._2.toList(1), x._1._2)))
  val subtractions = stationsPair.subtractByKey(saStations)
  val saDailyTemps = dailyIncrease.subtractByKey(subtractions)
  val highestIncrease = saDailyTemps.fold(("",(0.0, 0)))((acc,report) => {
    if (acc._2._1 < report._2._1) report else acc})

  System.out.println(highestIncrease)
  


}
