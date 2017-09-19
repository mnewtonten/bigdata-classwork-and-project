package sparkrdd2

import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scalafx.application.JFXApp
import org.apache.spark._
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.FXRenderer

object BetweenClassWork2_1c extends JFXApp {

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

  //val highTemps = sData.filter(_.elem == "TMAX").map(_.value/10)

  val valuePair = sData.map(x => (x.id) -> (x.elem,x.value))
  val maxTemps = valuePair.filter(x => (x._2._1 == "TMAX" && x._1.substring(0,2) == "US"))

  val groupOne = stations.filter(_.lat < 35).map(x => (x.id) -> (x.lat))
  val groupTwo = stations.filter(x => (x.lat > 35 && x.lat < 42 )).map(x => (x.id) -> (x.lat))
  val groupThree = stations.filter(_.lat > 42).map(x => (x.id) -> (x.lat))

  val highOne = maxTemps.join(groupOne).map((_._2._1._2/10))
  val highTwo = maxTemps.join(groupTwo).map((_._2._1._2/10))
  val highThree = maxTemps.join(groupThree).map((_._2._1._2/10))

  
  val bins = (-50.0 to 80.0 by 1.0).toArray
  var hist =  highOne.histogram(bins, true)
  var plot = Plot.histogramPlot(bins, hist, RedARGB, false, "High Temps for Group One", "Temp", "Count")
  FXRenderer(plot)

  hist =  highTwo.histogram(bins, true)
  plot = Plot.histogramPlot(bins, hist, RedARGB, false, "High Temps for Group Two", "Temp", "Count")
  FXRenderer(plot)

  hist =  highThree.histogram(bins, true)
  plot = Plot.histogramPlot(bins, hist, RedARGB, false, "High Temps for group Three", "Temp", "Count")
  FXRenderer(plot)

   

}
