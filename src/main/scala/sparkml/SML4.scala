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
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.evaluation.RegressionEvaluator


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
        }else if (movie_on > 5000){
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
  
  val limReviews = reviews.filter('UserID < 100000).cache()

  val Array(training, test) = limReviews.randomSplit(Array(0.8, 0.2)).map(_.cache())
  val als = new ALS()
    .setMaxIter(5)
    .setRegParam(0.01)
    .setUserCol("UserID")
    .setItemCol("MovieID")
    .setRatingCol("Rating")
  val model = als.fit(training)

  val recommendations = model.recommendForAllUsers(5)
  recommendations.show(false)

  val recMovies = recommendations.select('recommendations).collect.map{r => 
    r.getAs[Seq[Row]]("recommendations").map(_.getInt(0))
  }.flatten.groupBy(x => x).mapValues(_.length).toSeq.sortWith(_._2 > _._2)
  recMovies.take(10) foreach println

  val predictions = model.transform(test)  

  val evaluator = new RegressionEvaluator()
    .setMetricName("rmse")
    .setLabelCol("Rating")
    .setPredictionCol("prediction")
  val rmse = evaluator.evaluate(predictions.na.drop())
  println(s"Root-mean-square error = $rmse")

    
 
}

