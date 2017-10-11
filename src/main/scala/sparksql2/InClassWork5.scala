package sparksql2


import scalafx.application.JFXApp
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.FXRenderer
import org.apache.spark.sql.SparkSession
//import spark.implicits._

object InClassWork5 extends JFXApp {
  
  case class Results(id:Double, votes_dem:Double, votes_gop:Double, total_votes:Double)

  case class ZipData(zip:String, lat: Double, lon: Double, city:String, state:String, county:String)

  val spark = SparkSession.builder.master("spark://pandora00:7077").getOrCreate
  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")

  val zips = spark.read.schema(Encoders.product[ZipData].schema).option("header", true).csv("/data/BigData/bls/zip_codes_states.csv").as[ZipData]
  val results = spark.read.schema(Encoders.product[Results].schema).option("header", true).csv("/data/BigData/bls/2016_US_County_Level_Presidential_Results.csv").as[Results]

  //val counties = zips.groupBy('state, 'county).agg(avg('lat) as "lat",avg('lon) as "lon").filter('lat.isNotNull && 'lon.isNotNull)

  //Question 1

  val counties = results.count().toDouble
  val republican = results.filter('votes_gop > 'votes_dem).count().toDouble

  println(republican / counties)

  //Question 2

  val largeMargin = results.filter((('votes_dem - 'votes_gop) / 'total_votes) >= 0.10).count().toDouble


  println(largeMargin / counties)

  //Question 3

  //val x = results.select('total_votes).collect.map(_.getDouble(0))
  //val y = results.select('votes_dem).collect.map(_.getDouble(0))
  //val plot = Plot.scatterPlot(x, y, "Election Results", "Total", "Dem", 3, BlackARGB)
  //FXRenderer(plot, 800, 600)


  //Question 4

  val lon = zips.select('lon).collect().map(_.getDouble(0))
  val lat = zips.select('lat).collect().map(_.getDouble(0))
  val plot2 = Plot.scatterPlot(lon, lat, "Election Results Zip", "Longitude", "Latitude", 3, BlackARGB)
  FXRenderer(plot2, 800, 600)

  //spark.stop()

}
