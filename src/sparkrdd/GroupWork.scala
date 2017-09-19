import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scalafx.application.JFXApp
import org.apache.spark._

object GroupWork extends JFXApp {

  val conf = new SparkConf().setAppName("Hello World").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val lines2017 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/2017.csv")
  val stationsLines = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/ghcnd-stations.txt")

//  lines2017.take(5) foreach println
//  stations.take(5) foreach println

  //Trying to make a Case Class for stations
  
  case class Stations(id: String, state: String, name: String)
  val stations = stationsLines.map{line =>
    Stations(line.substring(0,11),line.substring(38,40),line.substring(41,71))
  }

  val txStationsCase = stations.filter(_.state == "TX")
  
   
  //Case Class for 2017 - and sData, RDD of Case Class SData
  case class SData(id: String, date: Int, elem: String, value: Int)
  //val source = io.Source.fromFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/2017.csv") - spark is bad
  //val lines = lines2017.getLines - does not work in spark, spark is bad
  val sData = lines2017.map {line => 
    val p = line.split(",")
    //Interesting idea - times out
    //val tempStation = stations.fold(Stations("","",""))((acc,state) => {if(state.state == "TX") state else acc})
    //val tempStation = Stations("","","")
    SData(p(0).toString, p(1).toInt, p(2).toString, p(3).toInt)
  }
  

  val stationIDs = stationsLines.map(_.substring(0,11))
  val txStations = stationsLines.filter(_.substring(38, 40) == "TX")
  val txStationsIDs = txStations.map(_.substring(0,11)).distinct()
  val reportIDs = lines2017.map(_.substring(0,11)).distinct()
  val txStationReportsIDs = txStationsIDs.intersection(reportIDs).distinct()
  val txStationReports = sData.filter(_.id.substring(3,5) == "TX")
  //stations that did not report
  val notReportStations = stationIDs.subtract(reportIDs)

  val precipData = sData.filter(_.elem == "PRCP")
  val tempData = sData.filter(x => (x.elem == "TMAX" || x.elem == "TMIN"))
  val tempDataIDs = tempData.map(_.id).distinct()

  val texasPrecipData = precipData.filter(_.id.substring(3,5) == "TX")
  val indiaPrecipData = precipData.filter(_.id.substring(0,2) == "IN")
  val sanAntonioStation = txStations.filter(_.substring(41,52) == "SAN ANTONIO")
  val sanAntonioStationID = sanAntonioStation.map(_.substring(0,11)).intersection(tempDataIDs).distinct()

  //for use with folds
  val dummySData = SData("dummy",0,"dummy",0)

  //Max temperature in 2017
  val maxTemp = sData.fold(dummySData)((acc,report) => {
  if(acc.value < report.value && report.elem == "TMAX") report else acc})
  
  //Max precipitation in Texas in 2017
  val maxPrecipTX = texasPrecipData.fold(dummySData)((acc,report) => {
  if(acc.value < report.value) report else acc})
  //Max precipitation in India in 2017
  val maxPrecipIN = indiaPrecipData.fold(dummySData)((acc,report) => {
  if(acc.value < report.value) report else acc})

  //println(txStations.count()+" stations in texas")
  //println(txStationReports.count()+" stations in texas that have reported") 
  //println("Max temp is "+maxTemp)
  //println(notReportStations.count()+" stations have not reported")
  //precipData.take(20) foreach println
  //println("Max rainfall in texas is "+maxPrecipTX)
  //println("Max rainfall in India is "+maxPrecipIN)
  //println(sanAntonioStation.count+" stations in San Antonio")
  //sanAntonioStation.take(5) foreach println
  println(sanAntonioStationID.count+" stations in San Antonio that have reported temp")
}
