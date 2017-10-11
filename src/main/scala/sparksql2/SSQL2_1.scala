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


object SSQL2_1 extends JFXApp{
  val spark = SparkSession.builder.master("spark://pandora00:7077").getOrCreate
  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")

  val schema = StructType(Array(
    StructField("series_id", StringType),
    StructField("year", IntegerType),
    StructField("period", StringType),
    StructField("value", DoubleType),
    StructField("footnote_codes", StringType)))

  val schema2 = StructType(Array(
    StructField("series_id", StringType),
    StructField("area_type_code", StringType)))
        

  val data = spark.read.schema(schema).option("header", true).option("delimiter", "\t").csv("/data/BigData/bls/la/la.data.concatenatedStateFiles").cache()
  val series = spark.read.schema(schema2).option("header",true).option("delimiter", "\t").csv("/data/BigData/bls/la/la.series").cache()

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

 
  //splitting up the datasets by date for question 1
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

  //making histogram plot grid for question 1
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
  FXRenderer(histPlot)

  
  




  System.out.println("~~L~O~O~K~~~O~U~T~~~B~E~L~O~W~~~B~I~G~~~D~A~T~A~~~I~N~C~O~M~I~N~G~~")



  System.out.println("~~W~H~E~W~~~T~H~A~T~~~W~A~S~~~S~O~M~E~~~B~I~G~~~D~A~T~A~~")


  spark.stop()

}
