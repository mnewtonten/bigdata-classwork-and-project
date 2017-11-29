import scalafx.application.JFXApp
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.ml._
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.types._
import org.apache.spark.sql.Encoders
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.functions._
import swiftvis2.plotting._
import swiftvis2.plotting.renderer._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.stat._
import org.apache.spark.ml.stat.Correlation
import scala.collection.mutable.ListBuffer  
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.sql.Row

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

object SML3 extends App {
  val spark = SparkSession.builder().master("spark://pandora00:7077").getOrCreate()
  import spark.implicits._

  //Logger.getLogger("org").setLevel(Level.OFF)
  spark.sparkContext.setLogLevel("WARN")

        val data = spark.read.option("header",false).option("delimiter", "\t").csv("/data/BigData/Netflix/combined_data_1.txt")

  
  
  spark.stop


  

}
