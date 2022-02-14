package predict

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Rating(user: Int, item: Int, rating: Double)

object Predictor extends App {
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
  println("Loading training data from: " + conf.train()) 
  val trainFile = spark.sparkContext.textFile(conf.train())
  val train = trainFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  }) 
  assert(train.count == 80000, "Invalid training data")

  println("Loading test data from: " + conf.test()) 
  val testFile = spark.sparkContext.textFile(conf.test())
  val test = testFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  }) 
  assert(test.count == 20000, "Invalid test data")

  // GLOBAL AVERAGE
  def computeGlobalAverageRating() = train.map(r => r.rating).mean()

  def maeGlobalMethod() = {
    val globalAverageRating = train.map(r => r.rating).mean()
    test.map(r => (r.rating - globalAverageRating).abs).mean
  }

  // PER USER
  def computeAvg(pairRDD: RDD[(Int, (Double, Int))]) = pairRDD.reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2))
                                  .mapValues(v  => v._1 / v._2)
  def computeTrainAvgRatingUser() = computeAvg(train.map(r =>  (r.user, (r.rating, 1))))
  def computeTrainAvgRatingItem() = computeAvg(train.map(r =>  (r.item, (r.rating, 1))))

  def computeMapMae(pairRDD:RDD[(Int, Double)], avgRating:RDD[(Int, Double)], globalAverageRating:Double) = pairRDD.leftOuterJoin(avgRating)
                                    .mapValues(x => x match {
                                        case (a, None) => (a - globalAverageRating).abs
                                        case (a, Some(b)) => (a - b).abs
                                    }).values.mean
  
  def perUserMethod() = {
    val globalAverageRating = train.map(r => r.rating).mean()
    val trainAvgRatingUser = computeAvg(train.map(r =>  (r.user, (r.rating, 1))))
    (globalAverageRating, trainAvgRatingUser)
  }
  def maePerUserMethod() = {
    val (globalAverageRating, trainAvgRatingUser) = perUserMethod()
    computeMapMae(test.map(r => (r.user, r.rating)), trainAvgRatingUser, globalAverageRating)
  }

  // PER ITEM
  def perItemMethod() = {
    val globalAverageRating = train.map(r => r.rating).mean()
    val trainAvgRatingItem = computeTrainAvgRatingItem()
    (globalAverageRating, trainAvgRatingItem)
  }
  def maePerItemMethod() = {
    val (globalAverageRating, trainAvgRatingItem) = perItemMethod()
    computeMapMae(test.map(r => (r.item, r.rating)), trainAvgRatingItem, globalAverageRating)
  }

  // BASELINE
  def scale(rating:Double, avgUserRating:Double) = rating match {
    case x if x > avgUserRating => 5 - avgUserRating
    case x if x < avgUserRating => avgUserRating - 1
    case _ => 1
  }

  def computeNormalizedDeviation(trainAvgRatingUser: RDD[(Int, Double)]) = train.map(r=>(r.user, (r.item, r.rating))).join(trainAvgRatingUser)
                            .mapValues({case ((item, rating), avg) => (item, (rating - avg) / scale(rating, avg).toDouble)}).values
  
  def computeAverageDeviationPerItem(normalizedDeviation: RDD[(Int, Double)]) = computeAvg(normalizedDeviation.map(r => (r._1, (r._2, 1))))

  def baselineMethod() = {
    val globalAverageRating = train.map(r => r.rating).mean()
    val trainAvgRatingUser = computeAvg(train.map(r =>  (r.user, (r.rating, 1))))
    val normalizedDeviation = computeNormalizedDeviation(trainAvgRatingUser)
    val averageDeviationPerItem = computeAverageDeviationPerItem(normalizedDeviation)
    test.map(r => (r.user, (r.item, r.rating))).join(trainAvgRatingUser)
        .map({case (user, ((item, rating), userAvg)) => (item, (rating, user, userAvg))})
        .leftOuterJoin(averageDeviationPerItem)
        .map({case (item, ((rating, user, userAvg), itemAvg)) => itemAvg match {
                case None => (rating, globalAverageRating)
                case Some(x) => (rating, userAvg + x * scale(userAvg + x, userAvg))
        }})
  }
  def maeBaselineMethod() = baselineMethod().map(r=> (r._1 - r._2).abs).mean()
    
  def microTime(func: () => Any) = {
    val start = System.nanoTime()
    func()
    val end = System.nanoTime()
    (end - start) / 1000
  }

  def measure(func: () => Any, n: Int = 10) = {
    val measurements = (1 to n).map(a => microTime(func))
    val min = measurements.min
    val max = measurements.max
    val average = measurements.sum / measurements.length
    val std = math.sqrt((measurements.map( _ - average).map(t => t*t).sum) / measurements.length)
    (min, max, average, std)
  }
  val globalMeasures = measure(computeGlobalAverageRating)
  val perUserMeasures = measure(perUserMethod)
  val perItemMeasures = measure(perItemMethod)
  val baselineMeasures = measure(() => baselineMethod.map(_._2).sum())  //Dummy action to trigger execution

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
            "Q3.1.4" -> Map(
              "MaeGlobalMethod" -> maeGlobalMethod(), // Datatype of answer: Double
              "MaePerUserMethod" -> maePerUserMethod(), // Datatype of answer: Double
              "MaePerItemMethod" -> maePerItemMethod(), // Datatype of answer: Double
              "MaeBaselineMethod" -> maeBaselineMethod() // Datatype of answer: Double
            ),

            "Q3.1.5" -> Map(
              "DurationInMicrosecForGlobalMethod" -> Map(
                "min" -> globalMeasures._1,  // Datatype of answer: Double
                "max" -> globalMeasures._2,  // Datatype of answer: Double
                "average" -> globalMeasures._3, // Datatype of answer: Double
                "stddev" -> globalMeasures._4 // Datatype of answer: Double
              ),
              "DurationInMicrosecForPerUserMethod" -> Map(
                "min" -> perUserMeasures._1,  // Datatype of answer: Double
                "max" -> perUserMeasures._2,  // Datatype of answer: Double
                "average" -> perUserMeasures._3, // Datatype of answer: Double
                "stddev" -> perUserMeasures._4 // Datatype of answer: Double
              ),
              "DurationInMicrosecForPerItemMethod" -> Map(
                "min" -> perItemMeasures._1,  // Datatype of answer: Double
                "max" -> perItemMeasures._2,  // Datatype of answer: Double
                "average" -> perItemMeasures._3, // Datatype of answer: Double
                "stddev" -> perItemMeasures._4 // Datatype of answer: Double
              ),
              "DurationInMicrosecForBaselineMethod" -> Map(
                "min" -> baselineMeasures._1,  // Datatype of answer: Double
                "max" -> baselineMeasures._2, // Datatype of answer: Double
                "average" -> baselineMeasures._3, // Datatype of answer: Double
                "stddev" -> baselineMeasures._4 // Datatype of answer: Double
              ),
              "RatioBetweenBaselineMethodAndGlobalMethod" -> baselineMeasures._3 / globalMeasures._3.toDouble // Datatype of answer: Double
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
