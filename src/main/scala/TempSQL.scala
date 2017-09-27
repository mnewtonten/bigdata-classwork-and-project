import scalafx.application.JFXApp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

//spark.sparkContext.setLogLevel("WARN")

object TempSQL extends JFXApp {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._

  val schema = StructType(Array(
    StructField("day", IntegerType),
    StructField("doy", IntegerType),    
    StructField("month", IntegerType),
    StructField("state", StringType),
    StructField("year", IntegerType),
    StructField("precip", DoubleType),
    StructField("tave", DoubleType),
    StructField("tmax", DoubleType),
    StructField("tmin", DoubleType)))

  val data = spark.read.schema(schema).option("header", true).
    csv("/users/mlewis/CSCI3395-F17/InClass/SanAntonioTemps.csv")

  data.show  
  data.select('year * 12 + 'month).show
  data.filter('precip > 1).show
  println(data.filter('tmax > 100).count)
  data.groupBy('year).mean("tmax").orderBy($"avg(tmax)").show
  data.describe().show()
  data.createOrReplaceTempView("tdata")
  spark.sql("select tmax from tdata").show()
  //spark.stop() 

}
