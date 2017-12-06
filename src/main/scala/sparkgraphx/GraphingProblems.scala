import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions._
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import scala.io.Source
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.functions.countDistinct
import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import scala.collection.mutable.HashMap


object GraphingProblems extends App{
  //val data_a = xml.XML.loadFile("/data/BigData/Medline/medsamp2016a.xml")
  //data_a.filter(_.contains("DescriptorName")) 
  
  val spark = SparkSession.builder().master("spark://pandora00:7077").getOrCreate()
  import spark.implicits._

 val data_a = Source.fromFile("/data/BigData/Medline/medsamp2016a.xml").getLines()
 val data_b = Source.fromFile("/data/BigData/Medline/medsamp2016b.xml").getLines()
 val data_c = Source.fromFile("/data/BigData/Medline/medsamp2016c.xml").getLines()
 val data_d = Source.fromFile("/data/BigData/Medline/medsamp2016d.xml").getLines()
 val data_e = Source.fromFile("/data/BigData/Medline/medsamp2016e.xml").getLines()
 val data_f = Source.fromFile("/data/BigData/Medline/medsamp2016f.xml").getLines()
 val data_g = Source.fromFile("/data/BigData/Medline/medsamp2016g.xml").getLines()
 val data_h = Source.fromFile("/data/BigData/Medline/medsamp2016h.xml").getLines()
 
 val data = data_a ++ data_b ++ data_c ++ data_d ++ data_e ++ data_f ++ data_g ++ data_h
 println("counting...")
 //println("distinct: " + data.filter(_.contains("DescriptorName")).map(x => x.split(">")(1).split("<")(0)).toSet.size)
 //Question 1 22547
 //2. What are the 10 most common descriptor names?
  var niceData = data.filter(_.contains("DescriptorName")).map(x => x.split(">")(1).split("<")(0)).toArray

  //niceData.take(5) foreach println
  println(niceData.size)
  val hm = HashMap[String, Int]()
  
  //var t = niceData.map(x => (x, niceData.count(_==x) ) ).toSet.toArray.sortWith(_._2 > _._2)


  var g = niceData.groupBy(x => x).map(x => (x._1, x._2.size) ).toArray.sortWith(_._2 > _._2).take(10) foreach println
  
 
 // println("SIZE: " + m.size)
 // println("SIZE: " + niceData.size)
 //println("10 top descriptors")
 //t.take(10) foreach println
  
  //Question 4. 508344662
  
  //Question 2.
  

}
