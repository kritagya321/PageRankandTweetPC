import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


object PageRank {
  def main(args: Array[String]): Unit = {
    if(args.length!=3){
      println("Usage: PageRank InputDir Count OutputDir")
    }

    //creating spark context and SparkSession
    val sc = new SparkContext(new SparkConf().setAppName("PageRank"))
    val spark = SparkSession.builder().appName("PgRank").getOrCreate()

    //reading input and RDD conversion
    val input = spark.read.option("header","true").option("inferSchema","true").csv(args(0)).drop("_c2")
    val inputRdd = input.rdd

    //Storing Data as K,V : where K = Origin Node, V = Destination Node
    val data = inputRdd.map(x=>(x(0).toString,x(1).toString))

    //Getting the distinct (Total) number of Airports.
    val distinctVal = data.map(x=>x._1).distinct.collect.toList

    //    Creating mutable collections such that we can get the values via its key and can change the values as well ("Mutable")
    val omutable = scala.collection.mutable.Map[String, Double]()
    var pmutable = scala.collection.mutable.Map[String, Double]()
    val RDD1 : RDD[Row] = input.select("ORIGIN").rdd
    val RDD2 : RDD[String]= RDD1.map(x=>x(0).toString)

    //outlinks calculation
    val outlinks1=RDD2.map(x=>(x,1)).reduceByKey((x,y)=>x+y).collect()

    //inserting outlinks into mutable Collection
    for ((k, v) <- outlinks1) {
      omutable.put(k, v)
    }
    //mutable collection for results
    for(c<-distinctVal){
      pmutable.put(c,10)
    }

    var PrTemp = scala.collection.mutable.Map[String, Double]()

    val loopCount = args(1)

    //Pagerank calculation
    for (itr <- 1 to loopCount.toInt) {
      for (c <- distinctVal) {
        PrTemp.put(c, 0)
      }
      for (c <- distinctVal) {
        pmutable.put(c, pmutable.get(c).getOrElse(Double).asInstanceOf[Double] / omutable.get(c).getOrElse(Double).asInstanceOf[Double])
      }
      for ((o, d) <- data.collect().toList) {
        PrTemp.put(d, PrTemp.get(d).getOrElse(Double).asInstanceOf[Double] + pmutable.get(o).getOrElse(Double).asInstanceOf[Double])
      }
      for ((c, v) <- PrTemp) {
        PrTemp.put(c, ((0.15 / distinctVal.size) + 0.85 * v))
      }
      pmutable = PrTemp.clone()
    }

    //sorting and saving the result
    val ranks = pmutable.toSeq.sortBy(-_._2)
    sc.parallelize(ranks).saveAsTextFile(args(2) + "")
  }
}
