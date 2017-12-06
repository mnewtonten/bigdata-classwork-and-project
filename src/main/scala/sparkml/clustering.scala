import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

object clustering extends App {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._
//  val directory = "/data/BigData/bls/la/"
  val directory = "/data/BigData/bls/qcew/"
  spark.sparkContext.setLogLevel("WARN")
  
//  val schemaNames = Array("area_fips","own_code","industry_code","agglvl_code","size_code","year","qtr","disclosure_code","qtrly_estabs","month1_emplvl","month2_emplvl","month3_emplvl","total_qtrly_wages","taxable_qtrly_wages","qtrly_contributions","avg_wkly_wage","lq_disclosure_code","lq_qtrly_estabs","lq_month1_emplvl","lq_month2_emplvl","lq_month3_emplvl","lq_total_qtrly_wages","lq_taxable_qtrly_wages","lq_qtrly_contributions","lq_avg_wkly_wage","oty_disclosure_code","oty_qtrly_estabs_chg","oty_qtrly_estabs_pct_chg","oty_month1_emplvl_chg","oty_month1_emplvl_pct_chg","oty_month2_emplvl_chg","oty_month2_emplvl_pct_chg","oty_month3_emplvl_chg","oty_month3_emplvl_pct_chg","oty_total_qtrly_wages_chg","oty_total_qtrly_wages_pct_chg","oty_taxable_qtrly_wages_chg","oty_taxable_qtrly_wages_pct_chg","oty_qtrly_contributions_chg","oty_qtrly_contributions_pct_chg","oty_avg_wkly_wage_chg","oty_avg_wkly_wage_pct_chg")
//  val schema = StructType(schemaNames.map(i => StructField(i,StringType)))
  
  
  val mainFile = spark.read.option("header", true).option("delimiter", ",").csv(directory + "2016.q1-q4.singlefile.csv")
  
//  mainFile.show()
  
  mainFile.filter('agglvl_code >= 70 && 'agglvl_code <= 78).groupBy('agglvl_code).count().show()

  
  val bexarFiles = mainFile.filter('area_fips === 48029)
  
  bexarFiles.describe("area_fips").show()
  
  mainFile.groupBy('industry_code).count().orderBy('count.desc).show()
  
  val counties = mainFile.filter('agglvl_code === 78)
//  counties.show()
//  
  counties.select('total_qtrly_wages).describe().show()
  
  val formatted = counties.select('industry_code, 'total_qtrly_wages.cast(IntegerType) as "total_qtrly_wages")
  
  val industries = formatted.groupBy('industry_code).sum("total_qtrly_wages")
  
  industries.orderBy($"sum(total_qtrly_wages)".desc).show()
  
  spark.close()

  println("~~~~~~~~~~~~~B~I~G~~~D~A~T~A~~~B~E~L~O~W~~~~~~~~~~~~~~~~~")
  
  //val vectorData = mainFile.rdd.map{x:Row => x.getAs[Vector](0)}
  

/*    ----- Method One Need to convert mainFile to RDD Vector format
  val numClusters = 2
  val numIterations = 20
  val clusters = KMeans.train(mainFile, numClusters, numIterations)
  val WSSSE = clusters.computeCost(mainFile)
*/

/*  ----- Method Two (Outdated?)
  val kmeans = new KMeans().setK(2).setSeed(1L)
  val model = kmeans.fit(mainFile)  //Breaks here, "fit" does not exist???
  val WSSSE = model.computeCost(mainFile)
*/

  //May need to convert to doubles??




  println("~~~~~~~~W~H~E~W~~~T~H~A~T~~~W~A~S~~~S~O~M~E~~~B~I~G~~~D~A~T~A~~~~~~~~") 


}
