import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scalafx.application.JFXApp
import org.apache.spark._
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.FXRenderer

object BetweenClassWork2 extends JFXApp {

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

  //for group question 1
  
  
  //These are for A.
  //val valuePair = sData.map(x => (x.id) -> (x.elem,x.value))
  //val maxTemps = valuePair.filter(x => (x._2._1 == "TMAX" && x._1.substring(0,2) == "US"))
  //val minTemps = valuePair.filter(x => (x._2._1 == "TMIN" && x._1.substring(0,2) == "US"))
  
  val valuePair = sData.map(x => (x.id,x.date) -> (x.elem,x.value))
  val maxTemps = valuePair.filter(x => (x._2._1 == "TMAX" && x._1._1.substring(0,2) == "US"))
  val minTemps = valuePair.filter(x => (x._2._1 == "TMIN" && x._1._1.substring(0,2) == "US"))
  val avgTemps = maxTemps.join(minTemps).map(x => ((x._1._1,(x._2._1._2 + x._2._2._2)/2)))
  
  val groupOne = stations.filter(_.lat < 35).map(x => (x.id) -> (x.lat))
  val groupTwo = stations.filter(x => (x.lat > 35 && x.lat < 42 )).map(x => (x.id) -> (x.lat))
  val groupThree = stations.filter(_.lat > 42).map(x => (x.id) -> (x.lat))
  
  val stdevOne = avgTemps.join(groupOne).map((_._2._1/10)).sampleStdev
  val stdevTwo = avgTemps.join(groupTwo).map((_._2._1/10)).sampleStdev
  val stdevThree = avgTemps.join(groupThree).map((_._2._1/10)).sampleStdev
  
  //val groupOneHighs = maxTemps.join(groupOne)
  //val groupTw

  //For part a.
  //val stdevOne = maxTemps.join(groupOne).map((_._2._1._2/10)).sampleStdev
  //val stdevTwo = maxTemps.join(groupTwo).map((_._2._1._2/10)).sampleStdev
  //val stdevThree = maxTemps.join(groupThree).map((_._2._1._2/10)).sampleStdev


  avgTemps.take(5) foreach println

  //Standard Deviations of high temperatures
  System.out.println("First group = "+stdevOne +", second group =  "+stdevTwo+", final group "+stdevThree)
  
}
