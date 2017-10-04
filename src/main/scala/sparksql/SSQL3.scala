import scalafx.application.JFXApp
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SSQL3 extends App{
  val spark = SparkSession.builder.master("spark://pandora00:7077").getOrCreate
  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")

  val schema = StructType(Array(
    StructField("series_id", StringType),
    StructField("year", IntegerType),
    StructField("period", StringType),
    StructField("value", DoubleType),
    StructField("footnote_codes", StringType)))

        

  val data = spark.read.schema(schema).option("header", true).option("delimiter", "\t").csv("/data/BigData/bls/la/la.data.concatenatedStateFiles").cache()

  val uRate = data.filter(substring('series_id,19,2) === "03").filter('period =!= "M13")
  val selURate = uRate.select(substring('series_id,0,18).as("ID"),'year,'period,'value )
  
  val lForce = data.filter(substring('series_id,19,2) === "06").filter('period =!= "M13").filter('value >= 10000)
  val selLForce = lForce.select(substring('series_id,0,18).as("ID"),'year,'period,'value.as("crap"))
  
  val combinedSHIT = selURate.join(selLForce, Seq("ID","year","period")).sort('value.desc).cache()


  
  


 
  
  //data.filter('precip > 2).select('year * 12 + 'month).show
  //println(data.filter('tmax > 100).count)
  //data.groupBy('year).mean("tmax").orderBy("avg(tmax)").show

  // question 2
  //data.filter(substring('series_id,19,2) === "04").select(max('value)).show()

  // question 3
  //println(data.filter(substring('series_id,0,1) === "G").count)

  System.out.println("~~L~O~O~K~~~O~U~T~~~B~E~L~O~W~~~B~I~G~~~D~A~T~A~~~I~N~C~O~M~I~N~G~~")

  // question 4.a
  data.filter(substring('series_id,19,2) === "03").filter('period =!= "M13").groupBy('period).mean("value").describe().show()

  // question 4.b
  data.filter(substring('series_id,19,2) === "03").filter('period === "M13").describe().show()
 
  // question 1.c
  //val monthlyUnemploymentRates = data.filter(substring('series_id,19,2) === "03").filter('period =!= "M13")
  //monthlyUnemploymentRates.describe()
  //System.out.println(monthlyUnemploymentRates)
  

  combinedSHIT.show

  System.out.println("~~W~H~E~W~~~T~H~A~T~~~W~A~S~~~S~O~M~E~~~B~I~G~~~D~A~T~A~~")


  // question 1.d

  //User defined function example
  //myFunc = udf(("stateToNum", (s:String) => s.filter(_ != '\'').toInt)
  //data.select(myFunc('state)).show()

/*
  user Defined Aggregation example
  typed & untyped
  object WeightedAverage extends Aggregator[TempData2,(Double,Double),Double] {
    def bufferEncoder
    daf finish(reduction: (Double,Double)):Double = r._1/r._2
    def merge(
    def reduce(b: (Double,Double), a:TempData2):(Double,Double) = {
      val (vsum,wsum = b
      (vsum + a.tmax*a.precip,wsum+a.precip)
    }
    def zero : (Double,Doube) = (0.0, 0.0)
  }
*/

  //data.createOrReplaceTempView("tdata")
  //spark.sql("select tmax from tdata").show

  spark.stop()

}
