package recommend

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

// import org.apache.spark.mllib.rdd.MLPairRDDFunctions.fromPairRDD

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
//   val personalFile = spark.sparkContext.textFile(conf.personal())
  val personalFile = spark.read.csv(conf.personal()).rdd.map(r => (r.getString(0).toInt, r.getString(1), r.getString(2)))
  val itemData = personalFile.map(l => (l._1, l._2))
  val personalData = personalFile.filter(l => l._3 != null).map(l => (l._1, l._2, l._3.toDouble))
  // TODO: Extract ratings and movie titles
  assert(personalFile.count == 1682, "Invalid personal data")

  val userData = data.union(personalData.map({case (item, title, rating) => Rating(944, item, rating)}))

  
  def computeAvg(pairRDD: RDD[(Int, (Double, Int))]) = pairRDD.reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2))
                                  .mapValues(v  => v._1 / v._2)

  def scale(rating:Double, avgUserRating:Double) = rating match {
    case x if x > avgUserRating => 5 - avgUserRating
    case x if x < avgUserRating => avgUserRating - 1
    case _ => 1
  }

  def computeNormalizedDeviation(trainAvgRatingUser: RDD[(Int, Double)]) = userData.map(r=>(r.user, (r.item, r.rating))).join(trainAvgRatingUser)
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

  def computeKSimilarity(preprocessedRatings: RDD[(Int, (Int, Double))], k: Int)={
    val itemRatings = preprocessedRatings.map({case (user, (item, rating)) => (item, (user, rating))}).cache()
    val similarities = itemRatings.join(itemRatings)
                .filter({case (item, ((u, uRating), (v, vRating))) => u != v})
                .map({case (item, ((u, uRating), (v, vRating))) => ((u,v), uRating * vRating)})
                .reduceByKey(_+_)
                .map({case ((u,v), similarity) => (u, (v, similarity))})
    topByKey(similarities, k)((a, b) => a._2.compare(b._2))
                .flatMap({case (u, topK) => topK.map({case (v, sim) => ((u, v), sim)})})
  }
  def knnPrediction(rdd: RDD[(Int, Int)], k: Int) = {
    val trainAvgRatingUser = computeAvg(userData.map(r =>  (r.user, (r.rating, 1)))).cache()
    val normalizedDeviation = computeNormalizedDeviation(trainAvgRatingUser)
    val preprocessedRatings = computePreprocessedRatings(normalizedDeviation)
    val kNearestSimilarities = computeKSimilarity(preprocessedRatings, k)
    val trainItemDeviation = normalizedDeviation.map({case (user, (item, rating)) => (item, (user, rating))})
    val userItemDeviation = rdd.map(r => (r._2, r._1))
                    .leftOuterJoin(trainItemDeviation) //Outer join if item not in data set
                    .map({case (item, (u, vPair)) => vPair match {
                        case Some(vPair) => ((u, vPair._1), (item, vPair._2))
                        case _ => ((u, -1), (item, 0.0)) //Set user id v to -1 s.t. next join removes it
                    }})
                    .join(kNearestSimilarities) //Case when no user with items in common treated later
                    .map({case ((u, v), ((item, vRating), sim)) => ((u, item), (sim * vRating, math.abs(sim)))})     
                    .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
                    .mapValues(r => r._1 / r._2)

    val globalAverageRating = userData.map(r => r.rating).mean()
    rdd.leftOuterJoin(trainAvgRatingUser) //Outer join if user not in data set
        .map({case (user, (item, avgRating)) => avgRating match {
            case Some(avgRating) => ((user, item), avgRating)
            case _ => ((user, item), globalAverageRating)
        }})
        .leftOuterJoin(userItemDeviation) // Outer join if no training user with items in common
        .map({case ((user, item), (avgRating, itemDeviation)) => itemDeviation match {
            case Some(itemDeviation) => (item, avgRating + itemDeviation * scale(avgRating + itemDeviation, avgRating))
            case _ => (item, avgRating)
        }})
    }
    
  def recommendations(k: Int = 30) = {
        val rdd = spark.sparkContext.parallelize((1 to 1682).map(item => (944, item)))
        val ratedMovies = personalData.map(r => r._1).collect()
        val recommendations = knnPrediction(rdd, k).filter(r => !ratedMovies.contains(r._1)) // only keep movies that I didn't rate
        itemData.map(r => (r._1, r._2)).join(recommendations).sortBy(r => (-r._2._2, r._1)) // sort by rating descendingly and by item id ascendingly
                .map({case (itemId, (title, rating)) => List(itemId, title, rating)}).take(5)
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

          "Q3.2.5" -> Map(
            "Top5WithK=30" -> recommendations(30),
              // Datatypes for answer: Int, String, Double
                // Representing: Movie Id, Movie Title, Predicted Rating
                // respectively

            "Top5WithK=300" -> recommendations(300)
            // Discuss the differences in rating depending on value of k in the report.
          )
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