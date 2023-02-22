import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object TweetPC {
  def main(args: Array[String]): Unit = {

    if(args.length!=2){
      println("Usage: TweetProcessing InputDir OutputDir")
    }

    //creating spark context and spark session
    val sc = new SparkContext(new SparkConf().setAppName("Tweet Analysis"))
    val spark = SparkSession.builder.appName("TweetAnalysis").getOrCreate()
    val sqlContxt = spark.sqlContext
    import spark.implicits._

    //reading data and filtering null entries
    val input = sqlContxt.read.format("com.databricks.spark.csv").option("header", "true").load(args(0))
    val cleanData = input.filter($"text".isNotNull)

    //importing and declaring the required libraries
    import org.apache.spark.ml.feature.{HashingTF, StopWordsRemover, StringIndexer, Tokenizer}

    val tokenize = new Tokenizer().setInputCol("text").setOutputCol("words")
    val stopwordRemover = new StopWordsRemover().setInputCol(tokenize.getOutputCol).setOutputCol("stopWords")
    val hashingTF = new HashingTF().setInputCol(stopwordRemover.getOutputCol).setOutputCol("features")
    val Labelencoder = new StringIndexer().setInputCol("airline_sentiment").setOutputCol("label")

    //using logistic regression
    import org.apache.spark.ml.classification.LogisticRegression

    val lrmodel = new LogisticRegression().setMaxIter(10).setFeaturesCol(hashingTF.getOutputCol).setLabelCol(Labelencoder.getOutputCol)

    import org.apache.spark.ml.Pipeline
    import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}

    val pipeline = new Pipeline().setStages(Array(tokenize, stopwordRemover, hashingTF, Labelencoder, lrmodel))
    val paramGrid = new ParamGridBuilder().addGrid(hashingTF.numFeatures, Array(10, 100, 1000)).addGrid(lrmodel.regParam, Array(0.1, 0.01)).build()
    import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction")
    val crossvalidate = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(3)


    //dividing the training and test sets
    val Array(training, test) = cleanData.randomSplit(Array(0.75,0.25))

    val trainModel = crossvalidate.fit(training)

    val testModel = trainModel.transform(test)

    val PredictionAndLabelsLR = testModel.select("prediction", "label").rdd.map{case Row(prediction: Double, label: Double) => (prediction, label)}

    import org.apache.spark.mllib.evaluation.MulticlassMetrics


    val metrics = new MulticlassMetrics(PredictionAndLabelsLR)

    var ResultTemp = "Tweet Processing & Classification using Pipelines\n"

    ResultTemp += "\nPrecision :    " + metrics.weightedPrecision
    ResultTemp += "\nRecall :   " + metrics.weightedRecall
    ResultTemp += "\nAccuracy :   " + metrics.accuracy
    ResultTemp += "\nF1Score :   " + metrics.weightedFMeasure


    val Result = List(ResultTemp)
    val outputRDD = sc.parallelize(Result)
    outputRDD.saveAsTextFile(args(1))

  }

}
