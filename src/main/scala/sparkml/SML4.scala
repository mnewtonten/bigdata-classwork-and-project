import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._



object SML4 extends App{
  val spark = SparkSession.builder().master("spark://pandora00:7077").getOrCreate
  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")


  case class Review(UserID: Int, Rating: Int, MovieID: Int)

  var lines = Source.fromFile("/data/BigData/Netflix/combined_data_1.txt").getLines()

  var movie = 0 
  var movie_on = 0 
  
  val reviews = spark.createDataset(
      lines.flatMap{line =>
        if(line.contains(":")){
           movie = line.replace(":","").toInt
           movie_on += 1
           None
        }else if (movie_on > 1000){
          None
        }else{
          var p = line.split(",")
          Some(Review(p(0).toInt,p(1).toInt, movie))
        }
      }.toArray
  )

  reviews.filter(_.UserID == 372233).map(_.Rating).describe().show()
  // 2649429 max
  // 6 min
  // 2649423  is the range
  // 404555 distinct userids



  println(reviews)


    
 
}

