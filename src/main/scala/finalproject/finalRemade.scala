import scalafx.application.JFXApp
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import scala.io.Source
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoders
import org.apache.spark.mllib.stat.Statistics
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.FXRenderer
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler


object finalRemade extends JFXApp{
  val spark = SparkSession.builder().master("spark://pandora00:7077").getOrCreate
  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")



  //Here I read in the Datasets
  val data1 = spark.read.option("header",true).option("multiline",true).csv("/users/mnewton/FinalProjectDatasets/articles1.csv")
  val data2 = spark.read.option("header",true).option("multiline",true).csv("/users/mnewton/FinalProjectDatasets/articles2.csv")
  val data3 = spark.read.option("header",true).option("multiline",true).csv("/users/mnewton/FinalProjectDatasets/articles3.csv")
  val sentimentTable = spark.sparkContext.textFile("/users/mnewton/FinalProjectDatasets/AFINN.txt").map(x=> x.split("\t")).map(x=>(x(0).toString, x(1).toInt))

  //Here I join all of the data 
  val data4 =  data1.select('title, 'publication, 'content).na.drop().unionAll(data2.select('title, 'publication, 'content).na.drop()).unionAll(data3.select('title, 'publication, 'content).na.drop())

  //This will help me add the columns that I want to 
  def addColumnIndex(df: org.apache.spark.sql.DataFrame) = spark.createDataFrame(
    // Add Column index
    df.rdd.zipWithIndex.map{case (row, columnindex) => Row.fromSeq(row.toSeq :+ columnindex)},
    // Create schema
    StructType(df.schema.fields :+ StructField("columnindex", LongType, false))
  )

  

  
 
  //Here I'm testing my methods
  val dataFilterContent = data1.select('title, 'publication, 'content)
  val dataNoNulls = dataFilterContent.na.drop()
   
  val sequence = dataNoNulls.select('content).collect().map(_.getString(0).length())
  val sequenceDF = spark.createDataset(sequence)
  val sequenceDF2 = addColumnIndex(sequenceDF.select('value.alias("characters").cast(DoubleType)))
  val sequenceDF3 = addColumnIndex(dataNoNulls)

  println(sequenceDF2.count())
  println(sequenceDF3.count())

  val sequenceJoin = sequenceDF2.join(sequenceDF3, Seq("columnindex"))

  val testingWords1 = sequenceJoin.select('content).limit(5).collect().map(_.getString(0)) 
  val testingWords2 = testingWords1.map(line => {
    val reviewWords = line.toString.split(" ").map(word => {
      (word.replaceAll("[,.!?:;]", "").trim.toLowerCase, line.toString.split(" ").count(_.replaceAll("[,.!?:;]", "").trim.toLowerCase == word.replaceAll("[,.!?:;]", "").trim.toLowerCase))
    })
    reviewWords.toSeq.distinct.filter(x => (x._1 != "the")&&(x._1 != "of")&&(x._1 != "to")&&(x._1 != "a")&&(x._1 != "in")&&(x._1 != "and")&&(x._1 != "that")&&(x._1 != "for")&&(x._1 != "at")&&(x._1 != "as")&&(x._1 != "with")&&(x._1 != "on")&&(x._1 != "is")&&(x._1 != "are")&&(x._1 != "who")&&(x._1 != "by")&&(x._1 != "it")&&(x._1 != "he")&&(x._1 != "she")&&(x._1 != "was")&&(x._1 != "an")&&(x._1 != "had")&&(x._1 != "his")&&(x._1 != "her")&&(x._1 != "but")).sortWith(_._2 > _._2)
  })

  //testingWords2.take(5) foreach println

  sequenceJoin.show()

  //I used this to figure out how many of each publication were in the datasets
  //data4.groupBy('publication).count.sort('count.desc).show
   
  //now I'm going to break down each of the publications and expand their datasets
  val breitbartArticles = data4.filter('publication === "Breitbart").limit(1000)
  val nypostArticles = data4.filter('publication === "New York Post").limit(1000)
  val nprArticles = data4.filter('publication === "NPR").limit(1000)
  val cnnArticles = data4.filter('publication === "CNN").limit(1000)
  val washingtonpostArticles = data4.filter('publication === "Washington Post").limit(1000)
  val reutersArticles = data4.filter('publication === "Reuters").limit(1000)
  val guardianArticles = data4.filter('publication === "Guardian").limit(1000)
  val nytimesArticles = data4.filter('publication === "New York Times").limit(1000)
  val atlanticArticles = data4.filter('publication === "Atlantic").limit(1000)
  val businessinsiderArticles = data4.filter('publication === "Business Insider").limit(1000)
  val nationalreviewArticles = data4.filter('publication === "National Review").limit(1000)
  val talkingpointsArticles = data4.filter('publication === "Talking Points Memo").limit(1000)
  val voxArticles = data4.filter('publication === "Vox").limit(1000)
  val buzzfeedArticles = data4.filter('publication === "Buzzfeed News").limit(1000)
  val foxArticles = data4.filter('publication === "Fox News").limit(1000)
 
  //Now I will organize these datasets based on their bias
  val veryConservativeArticles = breitbartArticles.unionAll(nationalreviewArticles).unionAll(foxArticles)
  val conservativeArticles = nypostArticles
  val neutralArticles = reutersArticles.unionAll(businessinsiderArticles).unionAll(buzzfeedArticles)
  val liberalArticles = nprArticles.unionAll(cnnArticles).unionAll(guardianArticles).unionAll(atlanticArticles).unionAll(talkingpointsArticles).unionAll(voxArticles)
  val veryLiberalArticles = washingtonpostArticles.unionAll(nytimesArticles)

  //making sure everything went through alright
  println(veryConservativeArticles.count())
  println(conservativeArticles.count())
  println(neutralArticles.count())
  println(liberalArticles.count())
  println(veryLiberalArticles.count())
 

  //Here I expand on each dataset, giving each entry number of characters, words, label, and sentiment value 
  def expandDataset(input: org.apache.spark.sql.DataFrame, label: Int):org.apache.spark.sql.DataFrame = {
  
    var tempCount1:Int = 0
    var tempCount2:Int = 0  
  
    val collectedContent = input.select('content).collect()
    val characters = collectedContent.map(_.getString(0).length())
    val brokenText = collectedContent.map(_.getString(0)).map(_.toString.split(" "))
    val words = brokenText.map(_.size)
    val labelSet = characters.map(x => (x-x+label))
    
    val makeSet1 = spark.createDataset(characters)
    val makeSet2 = spark.createDataset(words)
    val makeSet3 = spark.createDataset(labelSet)
    
    val addSet1 = addColumnIndex(input)
    val addSet2 = addColumnIndex(makeSet1.select('value.alias("characters").cast(DoubleType)))
    val addSet3 = addColumnIndex(makeSet2.select('value.alias("words").cast(DoubleType)))
    val addSet4 = addColumnIndex(makeSet3.select('value.alias("label")))

    val join1 = addSet1.join(addSet2, Seq("columnindex"))
    val join2 = join1.join(addSet3, Seq("columnindex"))
    val join3 = join2.join(addSet4, Seq("columnindex"))

    val mostWords = brokenText.map(line => {
      tempCount1 = tempCount1 + 1
      println(" at line "+ tempCount1 + " of label "+label)
      val articleWords = line.map(word => {

        (word.replaceAll("[,.!?:;]", "").trim.toLowerCase, line.count(_.replaceAll("[,.!?:;]", "").trim.toLowerCase == word.replaceAll("[,.!?:;]", "").trim.toLowerCase))
      })
      articleWords.toSeq.distinct.filter(x => (x._1 != "the")&&(x._1 != "of")&&(x._1 != "to")&&(x._1 != "a")&&(x._1 != "in")&&(x._1 != "and")&&(x._1 != "that")&&(x._1 != "for")&&(x._1 != "at")&&(x._1 != "as")&&(x._1 != "with")&&(x._1 != "on")&&(x._1 != "is")&&(x._1 != "are")&&(x._1 != "who")&&(x._1 != "by")&&(x._1 != "it")&&(x._1 != "he")&&(x._1 != "she")&&(x._1 != "was")&&(x._1 != "an")&&(x._1 != "had")&&(x._1 != "his")&&(x._1 != "her")&&(x._1 != "but")).sortWith(_._2 > _._2)
    })
    
    val sentimentValue = mostWords.map(line => {
      tempCount2 = tempCount2 + 1
      println(" at line "+tempCount2+ " of label "+label+ " working on sentiments now")
      val sentSeq = line.take(80).map(word => {
        var sentiment:Int = 0
        if(sentimentTable.lookup(word._1).length > 0){
            sentiment = sentimentTable.lookup(word._1)(0)
            sentiment = sentiment * (word._2)
        }
        else {sentiment = 0}
        sentiment
      })
      val totalSentiment = sentSeq.sum
      totalSentiment
    })
  
    val makeSet4 = spark.createDataset(sentimentValue)
    val addSet5 = addColumnIndex(makeSet4.select('value.alias("sentiment").cast(DoubleType)))
    val join4 = join3.join(addSet5, Seq("columnindex"))
    
  
    

    join4
  }

  //val test1 = expandDataset(conservativeArticles, 4 )
  //test1.show
  
  //Now I'll expand all of my datasets 
  val expandedVeryConservative = expandDataset(veryConservativeArticles, 5)
  val expandedConservative = expandDataset(conservativeArticles, 4)
  val expandedNeutral = expandDataset(neutralArticles, 3)
  val expandedLiberal = expandDataset(liberalArticles, 2)
  val expandedVeryLiberal = expandDataset(veryLiberalArticles, 1)

  //plot all datasets on sentiment vs word count
  
  val vcx = expandedVeryConservative.select('words).collect().map(_.getDouble(0))
  val vcy = expandedVeryConservative.select('sentiment).collect().map(_.getDouble(0))
  
  val cx = expandedConservative.select('words).collect().map(_.getDouble(0))
  val cy = expandedConservative.select('sentiment).collect().map(_.getDouble(0))
  
  val nx = expandedNeutral.select('words).collect().map(_.getDouble(0))
  val ny = expandedNeutral.select('sentiment).collect().map(_.getDouble(0))

  val lx = expandedLiberal.select('words).collect().map(_.getDouble(0))
  val ly = expandedLiberal.select('sentiment).collect().map(_.getDouble(0))

  val vlx = expandedVeryLiberal.select('words).collect().map(_.getDouble(0))
  val vly = expandedVeryLiberal.select('sentiment).collect().map(_.getDouble(0))

  val plotVC = Plot.scatterPlot(vcx, vcy, "Very Conservative", "Word Count", "Sentiment")
  val plotC = Plot.scatterPlot(cx, cy, "Conservative", "Word Count", "Sentiment")
  val plotN = Plot.scatterPlot(nx, ny, "Neutral", "Word Count", "Sentiment")
  val plotL = Plot.scatterPlot(lx, ly, "Liberal", "Word Count", "Sentiment")
  val plotVL = Plot.scatterPlot(vlx, vly, "Very Liberal", "Word Count", "Sentiment")

  FXRenderer(plotVC)
  FXRenderer(plotC)
  FXRenderer(plotN)
  FXRenderer(plotL)
  FXRenderer(plotVL)
  
  val masterSet = expandedVeryConservative.select('characters, 'words, 'sentiment, 'label).unionAll(expandedConservative.select('characters, 'words, 'sentiment, 'label)).unionAll(expandedNeutral.select('characters, 'words, 'sentiment, 'label)).unionAll(expandedNeutral.select('characters, 'words, 'sentiment, 'label)).unionAll(expandedLiberal.select('characters, 'words, 'sentiment, 'label)).unionAll(expandedVeryLiberal.select('characters, 'words, 'sentiment, 'label))

  val assembler = new VectorAssembler().setInputCols(masterSet.drop('label).columns).setOutputCol("features")
  val assembledData = assembler.transform(masterSet)
  
  val Array(trainData, testData) = assembledData.randomSplit(Array(0.7, 0.3)).map(_.cache())

  val rf = new RandomForestClassifier
  val model = rf.fit(trainData)

  val predictions = model.transform(testData)

  val evaluator = new MulticlassClassificationEvaluator
  val accuracy = evaluator.evaluate(predictions)
  println(s"accuracy = $accuracy")







  spark.stop

  }


