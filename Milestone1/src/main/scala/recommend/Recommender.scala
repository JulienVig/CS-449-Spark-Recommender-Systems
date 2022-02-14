package recommend

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val data = opt[String](required = true)
  val personal = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Rating(user: Int, item: Int, rating: Double)

object Recommender extends App {
  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR") 

  println("")
  println("******************************************************")

  var conf = new Conf(args) 
  println("Loading data from: " + conf.data()) 
  val dataFile = spark.sparkContext.textFile(conf.data())
  val data = dataFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  }) 
  assert(data.count == 100000, "Invalid data")

  println("Loading personal data from: " + conf.personal()) 
  val personalFile = spark.read.csv(conf.personal()).rdd.map(r => (r.getString(0).toInt, r.getString(1), r.getString(2)))
  val itemData = personalFile.map(l => (l._1, l._2))
  val personalData = personalFile.filter(l => l._3 != null).map(l => (l._1, l._2, l._3.toDouble))
  assert(personalFile.count == 1682, "Invalid personal data") 

  val userData = data.union(personalData.map({case (item, title, rating) => Rating(944, item, rating)}))

  def computeGlobalAverageRating() = userData.map(r => r.rating).mean()
  def computeAvg(pairRDD: RDD[(Int, (Double, Int))]) = pairRDD.reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2))
                                                            .mapValues(v  => v._1 / v._2)
  def computeTrainAvgRatingUser() = computeAvg(userData.map(r =>  (r.user, (r.rating, 1))))
  def computeNormalizedDeviation(trainAvgRatingUser: RDD[(Int, Double)]) = userData.map(r=>(r.user, (r.item, r.rating))).join(trainAvgRatingUser)
                            .mapValues({case ((item, rating), avg) => (item, (rating - avg) / scale(rating, avg).toDouble)}).values
  def computeAverageDeviationPerItem(normalizedDeviation: RDD[(Int, Double)]) = computeAvg(normalizedDeviation.map(r => (r._1, (r._2, 1))))
   def scale(rating:Double, avgUserRating:Double) = rating match {
    case x if x > avgUserRating => 5 - avgUserRating 
    case x if x < avgUserRating => avgUserRating - 1
    case _ => 1
  } 
  def baselinePrediction(rdd: RDD[(Int, Int)]) = {
    val globalAverageRating = computeGlobalAverageRating()
    val trainAvgRatingUser = computeTrainAvgRatingUser()
    val normalizedDeviation = computeNormalizedDeviation(trainAvgRatingUser)
    val averageDeviationPerItem = computeAverageDeviationPerItem(normalizedDeviation)
    rdd.join(trainAvgRatingUser)
        .map({case (user, (item, userAvg)) => (item, (user, userAvg))})
        .leftOuterJoin(averageDeviationPerItem)
        .map({case (item, ((user, userAvg), itemAvg)) => itemAvg match {
                case None => (item, globalAverageRating)
                case Some(x) => (item, userAvg + x * scale(userAvg + x, userAvg))
        }})
  }

  val popularity = userData.map(r => (r.item, 1)).reduceByKey(_+_).cache()
  val maxLogPopularity = math.log(popularity.sortBy(_._2, false).values.take(1)(0))

  def improvedPrediction(rdd: RDD[(Int, Int)]) = {
      baselinePrediction(rdd).join(popularity).map({case (item, (prediction, popularity)) => 
                        (item, prediction - (maxLogPopularity - math.log(popularity)) / maxLogPopularity)})
  }

  def recommendations(userId: Int = 944, n:Int = 5, improved: Boolean = false) = {
        val rdd = spark.sparkContext.parallelize((1 to 1682).map(item => (userId, item)))
        val ratedMovies = personalData.map(r => r._1).collect()
        val recommendations = (if(improved) improvedPrediction(rdd) else baselinePrediction(rdd)).filter(r => !ratedMovies.contains(r._1)) // only keep movies that I didn't rate
        itemData.map(r => (r._1, r._2)).join(recommendations).sortBy(r => (-r._2._2, r._1)) // sort by rating descendingly and by item id ascendingly
                .map({case (itemId, (title, rating)) => List(itemId, title, rating)}).take(n)
  }

  // Save answers as JSON
  def printToFile(content: String, 
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach{
      f => try{
        f.write(content)
      } finally{ f.close }
  }
  conf.json.toOption match {
    case None => ; 
    case Some(jsonFile) => {
      var json = "";
      {
        // Limiting the scope of implicit formats with {}
        implicit val formats = org.json4s.DefaultFormats
        val answers: Map[String, Any] = Map(

            // IMPORTANT: To break ties and ensure reproducibility of results,
            // please report the top-5 recommendations that have the smallest
            // movie identifier.

            "Q4.1.1" -> recommendations(),
            "Q4.1.2" -> recommendations(n = 10, improved = true)
         )
        json = Serialization.writePretty(answers)
      }

      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json, jsonFile)
    }
  }

  println("")
  spark.close()
}