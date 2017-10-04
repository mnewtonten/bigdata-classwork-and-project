import scalafx.application.JFXApp
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SSQLGroupWork extends App{
  val spark = SparkSession.builder.master("local[*]").getOrCreate
  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")

  val schema = StructType(Array(
    StructField("series_id", StringType),
    StructField("year", IntegerType),
    StructField("period", StringType),
    StructField("value", DoubleType),
    StructField("footnote_codes", StringType)))

  val data = spark.read.schema(schema).option("header", true).option("delimiter", "\t").csv("/data/BigData/bls/la/la.data.38.NewMexico")

  //data.filter('precip > 2).select('year * 12 + 'month).show
  //println(data.filter('tmax > 100).count)
  //data.groupBy('year).mean("tmax").orderBy("avg(tmax)").show

  // question 2
  //data.filter(substring('series_id,19,2) === "04").select(max('value)).show()

  // question 3
  //println(data.filter(substring('series_id,0,1) === "G").count)

  // question 4.a
  data.filter(substring('series_id,19,2) === "03").filter('period =!= "M13").groupBy('period).mean("value").describe().show()

  // question 4.b
  data.filter(substring('series_id,19,2) === "03").filter('period === "M13").describe().show()
  
  //data.createOrReplaceTempView("tdata")
  //spark.sql("select tmax from tdata").show

  spark.stop()
}
