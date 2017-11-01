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

object SML2 extends App {
  val spark = SparkSession.builder().master("spark://pandora00:7077").getOrCreate()
  import spark.implicits._

  //Logger.getLogger("org").setLevel(Level.OFF)
  spark.sparkContext.setLogLevel("WARN")

        val data = spark.read.option("header",false).option("delimiter", "\t").csv("/data/BigData/admissions/AdmissionAnon.tsv")

   //data.show()
  
  //val dealiasedUserArtistData = userArtistData.map(uad => uad.copy(artistID = 
  //val broadcastAliases = spark.sparkContext.broadcast(aliases)
  //sends only one copy instead of multiple

  //setRank() - sets associacions ex. genres of music

 println(data.count())

  data.agg(countDistinct("_c46")).show()

  data.groupBy("_c46").count().show()

  val colNames = new ListBuffer[String]()

  for (i <- 0 to 46) {
       if (i < 4 || i > 7) {
        colNames += "_c" + i
        }
  }

  val cols = ((0 to 4) ++ (8 to 46)).map(i => col("_c" + i).cast(DoubleType))


 //val ddf = data.select(cols:_*).cache().na.fill(0.0)
 

  val ddf = data.select(cols:_*).cache().na.fill(0.0).withColumn("label", '_c46)
  val ddf2 = data.select(cols:_*).cache().na.fill(0.0).withColumn("label", when('_c46 === 0 || '_c46 === 1, 1)otherwise(0))
  
  val assembler = new VectorAssembler().
    setInputCols(colNames.toArray).
    setOutputCol("features")
  val assembledData = assembler.transform(ddf)

  val assembler2 = new VectorAssembler().
    setInputCols(colNames.toArray).
    setOutputCol("features")
  val assembledData2 = assembler.transform(ddf2)



  //val df = assembledData.map(Tuple1.apply).toDF("features")
  

  //val corrMatrix = Correlation.corr(assembledData2, "features").head
  val Row(coeff1: Matrix) = Correlation.corr(assembledData, "features").head
  println("Pearson correlation matrix:\n" + coeff1.toString)
  println("--------------- No. 2 --------------------")
  //corrMatrix.show()
  coeff1.toArray.grouped(44).map(_.mkString(" ")).foreach(println)
 
  
  val Array(trainData, testData) = assembledData.randomSplit(Array(0.8, 0.2)).map(_.cache())
  val rf = new RandomForestClassifier
  val model = rf.fit(trainData)
   
  val predictions = model.transform(testData)
  predictions.show()
  val evaluator = new BinaryClassificationEvaluator
  val accuracy = evaluator.evaluate(predictions)
  println(s"accuracy = $accuracy")

  //NUMBA TWO 
  val Array(trainData2, testData2) = assembledData2.randomSplit(Array(0.8, 0.2)).map(_.cache())
  val rf2 = new RandomForestClassifier
  val model2 = rf2.fit(trainData)
   
  val predictions2 = model.transform(testData2)
  val evaluator2 = new BinaryClassificationEvaluator
  val accuracy2 = evaluator2.evaluate(predictions2)
  println(s"accuracy2 = $accuracy2")



  spark.stop


  

}
