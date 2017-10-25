import scalafx.application.JFXApp
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Encoders
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.functions._
import swiftvis2.plotting._
import swiftvis2.plotting.renderer._
import org.apache.spark.sql.SparkSession


object SML extends JFXApp{
  val spark = SparkSession.builder.master("spark://pandora00:7077").getOrCreate
  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")

  //case class (genHealth:Double, physHealth:Double, mentHealth:Double, poorHealth:Double, exeRany2:Double, slepTim1:Double)

  
/*
  rd.map{line =>
    cpls.map(c => line.substring(c.start-1, c.start+c.length))
  }
*/

  val schema = StructType(Array(
    StructField("genHealth", DoubleType),
    StructField("physHealth", DoubleType),
    StructField("mentHealth", DoubleType),
    StructField("poorHealth", DoubleType),
    StructField("exeRany2", DoubleType),
    StructField("slepTim1", DoubleType)))

  val schema2 = StructType(Array(
    StructField("series_id", StringType),
    StructField("area_type_code", StringType),
    StructField("area_code", StringType),
    StructField("measure_code",IntegerType),
    StructField("seasonal", StringType),
    StructField("srd_code",IntegerType),
    StructField("series_title",StringType)))
  
        
//  val election = spark.read.schema(Encoders.product[Results].schema).option("header",true).csv("/data/BigData/bls/2016_US_County_Level_Presidential_Results.csv").as[Results]
//  val data = spark.read.schema(schema).option("header", true).option("delimiter", "\t").csv("/data/BigData/bls/la/la.data.concatenatedStateFiles").cache()

//  val series = spark.read.schema(schema2).option("header",true).option("delimiter", "\t").csv("/data/BigData/bls/la/la.series").cache()
  val series = spark.read.schema(schema).option("header",false).option("delimiter", "\t").csv("/data/BigData/brfss/LLCLP2016.asc").cache()

  /*
  //getting Unemployment Rate  
  val uRate = data.filter(substring('series_id,19,2) === "03").filter('period =!= "M13")
  
  //creating Metro, Micro, and County datasets
  val metroIDs = series.filter('area_type_code === "B")
  val microIDs = series.filter('area_type_code === "D") 
  val countyIDs = series.filter('area_type_code === "F")
  val metroData = uRate.join(metroIDs, uRate("series_id") === metroIDs("series_id"))
  val microData = uRate.join(microIDs, uRate("series_id") === microIDs("series_id"))
  val countyData = uRate.join(countyIDs, uRate("series_id") === countyIDs("series_id"))
 
  countyData.show()
  */
 
  //splitting up the datasets by date for question 1
  /*
  val recessionAStartMetro = metroData.filter('year === 1990 && 'period === "M06").collect().map(_.getDouble(3))
  val recessionAEndMetro =  metroData.filter('year === 1991 && 'period === "M03").collect().map(_.getDouble(3))
  val recessionAStartMicro =  microData.filter('year === 1990 && 'period === "M06").collect().map(_.getDouble(3))
  val recessionAEndMicro =  microData.filter('year === 1991 && 'period === "M03").collect().map(_.getDouble(3))
  val recessionAStartCounties =  countyData.filter('year === 1990 && 'period === "M06").collect().map(_.getDouble(3))
  val recessionAEndCounties =  countyData.filter('year === 1991 && 'period === "M03").collect().map(_.getDouble(3))

  val recessionBStartMetro = metroData.filter('year === 2001 && 'period === "M02").collect().map(_.getDouble(3))
  val recessionBEndMetro = metroData.filter('year === 2001 && 'period === "M11").collect().map(_.getDouble(3))
  val recessionBStartMicro = microData.filter('year === 2001 && 'period === "M02").collect().map(_.getDouble(3))
  val recessionBEndMicro = microData.filter('year === 2001 && 'period === "M11").collect().map(_.getDouble(3))
  val recessionBStartCounties = countyData.filter('year === 2001 && 'period === "M02").collect().map(_.getDouble(3))
  val recessionBEndCounties = countyData.filter('year === 2001 && 'period === "M11").collect().map(_.getDouble(3))

  val recessionCStartMetro = metroData.filter('year === 2007 && 'period === "M11").collect().map(_.getDouble(3))
  val recessionCEndMetro = metroData.filter('year === 2009 && ('period === "M06")).collect().map(_.getDouble(3))
  val recessionCStartMicro = microData.filter('year === 2007 && 'period === "M11").collect().map(_.getDouble(3))
  val recessionCEndMicro = microData.filter('year === 2009 && 'period === "M06").collect().map(_.getDouble(3))
  val recessionCStartCounties = countyData.filter('year === 2007 && 'period === "M11").collect().map(_.getDouble(3))
  val recessionCEndCounties = countyData.filter('year === 2009 && 'period === "M06").collect().map(_.getDouble(3))
  */

  //making histogram plot grid for question 1
  /*
  val bins = 0.0 to 50.0 by 1.0
  val histPlot = Plot.histogramGrid(bins, 
      Seq(Seq((recessionAStartMetro, GreenARGB), (recessionAEndMetro, RedARGB),
       (recessionAStartMicro, GreenARGB), (recessionAEndMicro, RedARGB), 
       (recessionAStartCounties, GreenARGB), (recessionAEndCounties, RedARGB)),
      Seq((recessionBStartMetro, GreenARGB), (recessionBEndMetro, RedARGB), 
       (recessionBStartMicro, GreenARGB), (recessionBEndMicro, RedARGB),
       (recessionBStartCounties, GreenARGB), (recessionBEndCounties, RedARGB)),     
      Seq((recessionCStartMetro, GreenARGB), (recessionCEndMetro, RedARGB), 
       (recessionCStartMicro, GreenARGB), (recessionCEndMicro, RedARGB),
       (recessionCStartCounties, GreenARGB), (recessionCEndCounties, RedARGB))), 
     true, false, "Unemployment During Recessions", "", "")
  //FXRenderer(histPlot)
  */

  //Question 2
  
  /*
  val voteSelect = election.select('total_votes, 'per_dem, 'per_gop, 'diff, 'county)
  
  val sTitleUDF = udf{(s:String) => s.split(":")(1).split(",")(0).reverse.dropRight(1).reverse}
  
  val splitData = countyData.filter('period === "M11" && 'year === 2016).withColumn("split_name", sTitleUDF('series_title))
  val demByCounty = splitData.join(voteSelect, 'split_name === 'county)
  val correlation = demByCounty.stat.corr("value", "per_dem")
  
  val uValues = demByCounty.select('value).collect().map(_.getDouble(0))
  val perDemValues = demByCounty.select('per_dem).collect().map(_.getDouble(0))
  val popScale = demByCounty.select('total_votes).collect().map(_.getDouble(0)).map(_ * 0.00001)
  
  val twoBPlot = Plot.scatterPlot(uValues, perDemValues, "Unemployment/Percent Dem Vote", "Unemployment","Percent Dem Vote", popScale )
  FXRenderer(twoBPlot, 1000, 1000)



  System.out.println("~~L~O~O~K~~~O~U~T~~~B~E~L~O~W~~~B~I~G~~~D~A~T~A~~~I~N~C~O~M~I~N~G~~")
  
  println("Correlation between Unemployment and Percent Democratic = "+ correlation)


  System.out.println("~~W~H~E~W~~~T~H~A~T~~~W~A~S~~~S~O~M~E~~~B~I~G~~~D~A~T~A~~")
  */

  spark.stop()

}
