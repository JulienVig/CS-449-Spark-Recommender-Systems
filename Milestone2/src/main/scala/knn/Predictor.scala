package knn

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import java.io.Serializable
import java.util.{PriorityQueue => JPriorityQueue}

import scala.collection.JavaConverters._
import scala.collection.generic.Growable

/**
 * Bounded priority queue. This class wraps the original PriorityQueue
 * class and modifies it such that only the top K elements are retained.
 * The top K elements are defined by an implicit Ordering[A].
 */
class BoundedPriorityQueue[A](maxSize: Int)(implicit ord: Ordering[A])
  extends Iterable[A] with Growable[A] with Serializable {

  //  Note: this class supports Scala 2.12. A parallel source tree has a 2.13 implementation.

  private val underlying = new JPriorityQueue[A](maxSize, ord)

  override def iterator: Iterator[A] = underlying.iterator.asScala

  override def size: Int = underlying.size

  override def ++=(xs: TraversableOnce[A]): this.type = {
    xs.foreach { this += _ }
    this
  }

  override def +=(elem: A): this.type = {
    if (size < maxSize) {
      underlying.offer(elem)
    } else {
      maybeReplaceLowest(elem)
    }
    this
  }

  def poll(): A = {
    underlying.poll()
  }

  override def +=(elem1: A, elem2: A, elems: A*): this.type = {
    this += elem1 += elem2 ++= elems
  }

  override def clear(): Unit = { underlying.clear() }

  private def maybeReplaceLowest(a: A): Boolean = {
    val head = underlying.peek()
    if (head != null && ord.gt(a, head)) {
      underlying.poll()
      underlying.offer(a)
    } else {
      false
    }
  }
}
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

  def computeAvg(pairRDD: RDD[(Int, (Double, Int))]) = pairRDD.reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2))
                                  .mapValues(v  => v._1 / v._2)

  def scale(rating:Double, avgUserRating:Double) = rating match {
    case x if x > avgUserRating => 5 - avgUserRating
    case x if x < avgUserRating => avgUserRating - 1
    case _ => 1
  }

  def computeNormalizedDeviation(trainAvgRatingUser: RDD[(Int, Double)]) = train.map(r=>(r.user, (r.item, r.rating))).join(trainAvgRatingUser)
                            .mapValues({case ((item, rating), avg) => (item, (rating - avg) / scale(rating, avg).toDouble)})
  
  def computePreprocessedRatings(normalizedDeviation: RDD[(Int, (Int, Double))]) = {
    val usersRSS = normalizedDeviation.mapValues(r => math.pow(r._2,2))
                                      .reduceByKey(_+_)
                                      .mapValues(math.sqrt(_))
    normalizedDeviation.join(usersRSS)
                        .map({case (user, ((item, rating), rss)) => 
                            (user, (item, rating / rss))
                        })
  }
  
  def precomputeSimilarity() = {
    val trainAvgRatingUser = computeAvg(train.map(r =>  (r.user, (r.rating, 1)))).cache()
    val normalizedDeviation = computeNormalizedDeviation(trainAvgRatingUser).cache()
    val preprocessedRatings = computePreprocessedRatings(normalizedDeviation)
    val itemRatings = preprocessedRatings.map({case (user, (item, rating)) => (item, (user, rating))}).cache()
    val similarities = itemRatings.join(itemRatings)
                .filter({case (item, ((u, uRating), (v, vRating))) => u != v})
                .map({case (item, ((u, uRating), (v, vRating))) => ((u,v), uRating * vRating)})
                .reduceByKey(_+_)
                .map({case ((u,v), similarity) => (u, (v, similarity))}).cache()
    (trainAvgRatingUser, normalizedDeviation, similarities)
  }

  def topByKey(rdd: RDD[(Int, (Int, Double))], num: Int)(implicit ord: Ordering[(Int, Double)]): RDD[(Int, Array[(Int, Double)])] = {
    rdd.aggregateByKey(new BoundedPriorityQueue[(Int, Double)](num)(ord))(
      seqOp = (queue, item) => {
        queue += item
      },
      combOp = (queue1, queue2) => {
        queue1 ++= queue2
      }
    ).mapValues(_.toArray.sorted(ord.reverse))  // This is an min-heap, so we reverse the order.
  }

  var currTopByKey = spark.sparkContext.emptyRDD[(Int, (Int, Double))]

  def computeKSimilarity(allSimilarities: RDD[(Int, (Int, Double))], k: Int)={
    currTopByKey = topByKey(if(k==943) allSimilarities else currTopByKey, k)((a, b) => a._2.compare(b._2))
                        .flatMap({case (u, topK) => topK.map({case (v, sim) => (u, (v, sim))})})
    currTopByKey.map({case (u,(v,sim)) => ((u,v), sim)})
  }

  def knnPrediction(trainAvgRatingUser: RDD[(Int, Double)], 
                    normalizedDeviation: RDD[(Int, (Int, Double))], 
                    similarities: RDD[(Int, (Int, Double))],
                    k: Int) = {
    val kNearestSimilarities = computeKSimilarity(similarities, k)
    val trainItemDeviation = normalizedDeviation.map({case (user, (item, rating)) => (item, (user, rating))})
    val userItemDeviation = test.map(r => (r.item, r.user))
                    .leftOuterJoin(trainItemDeviation) //Outer join if item not in train set
                    .map({case (item, (u, vPair)) => vPair match {
                        case Some(vPair) => ((u, vPair._1), (item, vPair._2))
                        case _ => ((u, -1), (item, 0.0)) //Set user id v to -1 s.t. next join removes it
                    }})
                    .join(kNearestSimilarities) //Case when no user with items in common treated later
                    .map({case ((u, v), ((item, vRating), sim)) => ((u, item), (sim * vRating, math.abs(sim)))})     
                    .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
                    .mapValues(r => r._1 / r._2)

    val globalAverageRating = train.map(r => r.rating).mean()
    test.map(r => (r.user, (r.item, r.rating)))
        .leftOuterJoin(trainAvgRatingUser) //Outer join if user not in train set
        .map({case (user, ((item, rating), avgRating)) => avgRating match {
            case Some(avgRating) => ((user, item), (rating, avgRating))
            case _ => ((user, item), (rating, globalAverageRating))
        }})
        .leftOuterJoin(userItemDeviation) // Outer join if no training user with items in common
        .map({case ((user, item), ((rating, avgRating), itemDeviation)) => itemDeviation match {
            case Some(itemDeviation) => (rating, avgRating + itemDeviation * scale(avgRating + itemDeviation, avgRating))
            case _ => (rating, avgRating)
        }})
    }

    def computeMaeKnn(k: Int) = {
        val (trainAvgRatingUser, normalizedDeviation, allSimilarities) = precomputeSimilarity()
        knnPrediction(trainAvgRatingUser, normalizedDeviation, allSimilarities, k)
                    .map(r => (r._1 - r._2).abs).mean()
    }

    val kvalues = List(10, 30, 50, 100, 200, 300, 400, 800, 943)
    val maeForK = kvalues.reverse.map(computeMaeKnn).reverse //Start by 943 and then decrease k

    val firstMae = maeForK.find(mae => mae < 0.7669) match {
        case Some(mae) => mae
        case _ => -1
    }
    val firstK = kvalues(maeForK.indexOf(firstMae))

    val nbUsers = train.map(r => (r.user, 1)).reduceByKey((a,b) => 1).count
    val nbBytes = kvalues.map(k => 8 * k * nbUsers)
    val ramSize =  8 * math.pow(10,9)

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
          "Q3.2.1" -> Map(
            // Discuss the impact of varying k on prediction accuracy on
            // the report.
            "MaeForK=10" -> maeForK(0), // Datatype of answer: Double
            "MaeForK=30" -> maeForK(1), // Datatype of answer: Double
            "MaeForK=50" -> maeForK(2), // Datatype of answer: Double
            "MaeForK=100" -> maeForK(3), // Datatype of answer: Double
            "MaeForK=200" -> maeForK(4), // Datatype of answer: Double
            "MaeForK=300" -> maeForK(5), // Datatype of answer: Double
            "MaeForK=400" -> maeForK(6), // Datatype of answer: Double
            "MaeForK=800" -> maeForK(7), // Datatype of answer: Double
            "MaeForK=943" -> maeForK(8), // Datatype of answer: Double
            "LowestKWithBetterMaeThanBaseline" -> firstK, // Datatype of answer: Int
            "LowestKMaeMinusBaselineMae" -> (firstMae - 0.7669) // Datatype of answer: Double
          ),

          "Q3.2.2" ->  Map(
            // Provide the formula the computes the minimum number of bytes required,
            // as a function of the size U in the report.
            "MinNumberOfBytesForK=10" ->  nbBytes(0), // Datatype of answer: Int
            "MinNumberOfBytesForK=30" ->  nbBytes(1), // Datatype of answer: Int
            "MinNumberOfBytesForK=50" ->  nbBytes(2), // Datatype of answer: Int
            "MinNumberOfBytesForK=100" ->  nbBytes(3), // Datatype of answer: Int
            "MinNumberOfBytesForK=200" ->  nbBytes(4), // Datatype of answer: Int
            "MinNumberOfBytesForK=300" ->  nbBytes(5), // Datatype of answer: Int
            "MinNumberOfBytesForK=400" ->  nbBytes(6), // Datatype of answer: Int
            "MinNumberOfBytesForK=800" ->  nbBytes(7), // Datatype of answer: Int
            "MinNumberOfBytesForK=943" ->  nbBytes(8) // Datatype of answer: Int
          ),

          "Q3.2.3" -> Map(
            "SizeOfRamInBytes" -> ramSize, // Datatype of answer: Int
            "MaximumNumberOfUsersThatCanFitInRam" -> math.floor(ramSize / (3 * 8 * firstK))  // Datatype of answer: Int
          )

          // Answer the Question 3.2.4 exclusively on the report.
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