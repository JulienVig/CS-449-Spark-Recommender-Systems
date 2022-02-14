package stats

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val data = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Rating(user: Int, item: Int, rating: Double)

object Analyzer extends App {
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
      val globalAverageRating = data.map(r => r.rating).mean()
      
      def computeAvg(pairRDD: RDD[(Int, (Double, Int))]) = pairRDD.reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2))
                                        .mapValues(v  => v._1 / v._2).values
      def differenceWithGlobalAverage(rdd: RDD[Double]) = rdd.map(avg => (avg - globalAverageRating).abs < 0.5)
      
      def avgRatingPerUser(rdd: RDD[Rating]) = computeAvg(rdd.map(r =>  (r.user, (r.rating, 1))))
      def avgRatingPerItem(rdd: RDD[Rating]) = computeAvg(rdd.map(r => (r.item, (r.rating, 1))))

      val dataAvgRatingPerUser = avgRatingPerUser(data).cache()
      val dataAvgRatingPerItem = avgRatingPerItem(data).cache()
      val usersDifferenceWithGlobalAverage = differenceWithGlobalAverage(avgRatingPerUser(data)).cache()
      val itemDifferenctWithGloablAverage = differenceWithGlobalAverage(avgRatingPerItem(data)).cache()

      def all(rdd: RDD[Boolean]) = rdd.reduce(_ && _) // Test if all the booleans are true
      def ratio(rdd: RDD[Boolean]) = rdd.map(b => if (b) 1 else 0).mean() // Ratio of True values

      var json = "";
      {
        // Limiting the scope of implicit formats with {}
        implicit val formats = org.json4s.DefaultFormats
        val answers: Map[String, Any] = Map(
          "Q3.1.1" -> Map(
            "GlobalAverageRating" ->  globalAverageRating// Datatype of answer: Double
          ),
          "Q3.1.2" -> Map(
            "UsersAverageRating" -> Map(
                // Using as your input data the average rating for each user,
                // report the min, max and average of the input data.
                "min" -> dataAvgRatingPerUser.min,  // Datatype of answer: Double
                "max" -> dataAvgRatingPerUser.max, // Datatype of answer: Double
                "average" -> dataAvgRatingPerUser.mean() // Datatype of answer: Double
            ),
            "AllUsersCloseToGlobalAverageRating" -> all(usersDifferenceWithGlobalAverage), // Datatype of answer: Boolean
            "RatioUsersCloseToGlobalAverageRating" -> ratio(usersDifferenceWithGlobalAverage) // Datatype of answer: Double
          ),
          "Q3.1.3" -> Map(
            "ItemsAverageRating" -> Map(
                // Using as your input data the average rating for each item,
                // report the min, max and average of the input data.
                "min" -> dataAvgRatingPerItem.min,  // Datatype of answer: Double
                "max" -> dataAvgRatingPerItem.max, // Datatype of answer: Double
                "average" -> dataAvgRatingPerItem.mean() // Datatype of answer: Double
            ),
            "AllItemsCloseToGlobalAverageRating" -> all(itemDifferenctWithGloablAverage), // Datatype of answer: Boolean
            "RatioItemsCloseToGlobalAverageRating" -> ratio(itemDifferenctWithGloablAverage) // Datatype of answer: Double
          ),
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
