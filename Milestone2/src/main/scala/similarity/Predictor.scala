package similarity

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

    def computeAvg(pairRDD: RDD[(Int, (Double, Int))]) = pairRDD.reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2))
                                    .mapValues(v  => v._1 / v._2)

    def scale(rating:Double, avgUserRating:Double) = rating match {
        case x if x > avgUserRating => 5 - avgUserRating
        case x if x < avgUserRating => avgUserRating - 1
        case _ => 1
    }

    def computeNormalizedDeviation(trainAvgRatingUser: RDD[(Int, Double)]) = {
        train.map(r=>(r.user, (r.item, r.rating)))
            .join(trainAvgRatingUser)
            .mapValues({case ((item, rating), avg) => (item, (rating - avg) / scale(rating, avg).toDouble)})
    }
    
    def computePreprocessedRatings(normalizedDeviation: RDD[(Int, (Int, Double))]) = {
        val usersRSS = normalizedDeviation.mapValues(r => math.pow(r._2,2))
                                        .reduceByKey(_+_)
                                        .mapValues(math.sqrt(_))
        normalizedDeviation.join(usersRSS)
                            .map({case (user, ((item, rating), rss)) => 
                                (user, (item, rating / rss))
                            })
    }

    def computeCosineSimilarity(preprocessedRatings: RDD[(Int, (Int, Double))])={
        val itemRatings = preprocessedRatings.map({case (user, (item, rating)) => (item, (user, rating))}).cache()
        itemRatings.join(itemRatings)
                    .map({case (item, ((u, uRating), (v, vRating))) => ((u,v), uRating * vRating)})
                    .reduceByKey(_+_)
    }

    def computeJaccardSimilarity() = {
        val userRatings = train.map(r => (r.user, 1)).reduceByKey(_+_)
        val itemRatings = train.map(r => (r.user, r.item)).join(userRatings)
                                .map({case (user, (item, uNbRatings)) => (item, (user, uNbRatings))}).cache()
        itemRatings.join(itemRatings)
                    .filter({case (item, ((u, _), (v, _))) => u != v})
                    .map({case (item, ((u, uNbRatings), (v, vNbRatings))) => ((u, v, uNbRatings, vNbRatings), 1)})
                    .reduceByKey(_+_)
                    .map({case ((u, v, uNbRatings, vNbRatings), uvInter) => ((u,v), uvInter.toDouble / (uNbRatings + vNbRatings - uvInter))})
    }

    def computeBaselineSimilarity() = {
        val itemRatings = train.map(r => (r.item, r.user)).cache
        itemRatings.join(itemRatings).map({case (item, (u, v)) => ((u, v), 1.0)})
                    .reduceByKey((a,b) => 1.0)
    }

    def computeSimilarity(similarityMethod: String = "Cosine") = {
        val trainAvgRatingUser = computeAvg(train.map(r =>  (r.user, (r.rating, 1)))).cache()
        val normalizedDeviation = computeNormalizedDeviation(trainAvgRatingUser)
        val similarities = {
            if (similarityMethod == "Cosine"){
                val preprocessedRatings = computePreprocessedRatings(normalizedDeviation)
                computeCosineSimilarity(preprocessedRatings)
            } else if (similarityMethod == "Jaccard"){
                computeJaccardSimilarity()
            } else {
                computeBaselineSimilarity() 
            } 
        }
        (trainAvgRatingUser, normalizedDeviation, similarities)
    }

    def predictionFromSimilarities(trainAvgRatingUser: RDD[(Int, Double)], 
                                    normalizedDeviation: RDD[(Int, (Int, Double))], 
                                    similarities: RDD[((Int, Int), Double)]) = {
        val trainItemDeviation = normalizedDeviation.map({case (user, (item, rating)) => (item, (user, rating))})
        val userItemDeviation = test.map(r => (r.item, r.user))
                        .leftOuterJoin(trainItemDeviation) //Outer join if item not in train set
                        .map({case (item, (u, vPair)) => vPair match {
                            case Some(vPair) => ((u, vPair._1), (item, vPair._2))
                            case _ => ((u, -1), (item, 0.0)) //Set user id v to -1 s.t. next join removes it
                        }})
                        .join(similarities) //Case when no user with items in common treated later
                        .map({case ((u, v), ((item, vRating), sim)) => ((u, item), (sim * vRating, math.abs(sim)))})     
                        .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
                        .mapValues(r => r._1 / r._2)
        val globalAverageRating = train.map(r => r.rating).mean()
        val predictions = test.map(r => (r.user, (r.item, r.rating)))
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
        predictions
    }

    def similarityPrediction(similarityMethod: String = "Cosine") = {
        val (trainAvgRatingUser, normalizedDeviation, similarities) = computeSimilarity(similarityMethod)
        val predictions = predictionFromSimilarities(trainAvgRatingUser, normalizedDeviation, similarities)
        predictions
    }
    var predictionsCosine = spark.sparkContext.emptyRDD[(Double, Double)]
    def timePrediction() = {
        val startSimilarity = System.nanoTime() 
        val (trainAvgRatingUser, normalizedDeviation, similarities) = computeSimilarity("Cosine")
        similarities.count
        val endSimilarity = System.nanoTime()
        val startPredictions = System.nanoTime()
        predictionsCosine = predictionFromSimilarities(trainAvgRatingUser, normalizedDeviation, similarities)
        predictionsCosine.count
        val endPredictions = System.nanoTime()

        val timeSimilarities = (endSimilarity - startSimilarity) / 1000
        val timePredictions = timeSimilarities + (endPredictions - startPredictions) / 1000
        (timeSimilarities, timePredictions)
    }
        

    def measure(func: () => (Long, Long), n: Int = 5) = {
        val (measureSimilarity, measurePredictions) = (1 to n).map(a => func()).unzip
        (statistics(measureSimilarity), statistics(measurePredictions))
    }

    def statistics(measurements: Seq[Long]) = {
        val min = measurements.min
        val max = measurements.max
        val average = measurements.sum / measurements.length
        val std = math.sqrt((measurements.map(_ - average).map(t => t*t).sum) / measurements.length)
        (min, max, average, std)
    }

    val (similarityMeasures, predictionMeasures) = measure(timePrediction, n=5)
    val maeCosine = predictionsCosine.map(r => (r._1 - r._2).abs).mean()
    val maeJaccard = similarityPrediction("Jaccard").map(r => (r._1 - r._2).abs).mean()

    val nbUsers = train.map(r => (r.user, 1)).reduceByKey((a,b) => 1).count
    val nbOfSuv = nbUsers * (nbUsers - 1) / 2
    val nbBytes = 8 * nbOfSuv

    def computeMinMultiplication() = {
        val itemRatings = train.map(r => (r.item, r.user)).cache
        val minMult = itemRatings.join(itemRatings)
                                .filter({case (item, (u,v)) => u!= v})
                                .map({case (item, (u, v)) => ((u,v), 1)})
                                .reduceByKey(_+_).values
        val average = minMult.mean
        val stddev = math.sqrt((minMult.map(_ - average).map(t => t*t).sum) / minMult.count)
        (minMult.min, minMult.max, average, stddev)
    }
    val minMult = computeMinMultiplication()

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
            "Q2.3.1" -> Map(
                "CosineBasedMae" -> maeCosine, // Datatype of answer: Double
                "CosineMinusBaselineDifference" -> (maeCosine - 0.7669) // Datatype of answer: Double
            ),

            "Q2.3.2" -> Map(
                "JaccardMae" -> maeJaccard, // Datatype of answer: Double
                "JaccardMinusCosineDifference" -> (maeJaccard - maeCosine) // Datatype of answer: Double
            ),

            "Q2.3.3" -> Map(
                // Provide the formula that computes the number of similarity computations
                // as a function of U in the report.
                "NumberOfSimilarityComputationsForU1BaseDataset" -> nbOfSuv // Datatype of answer: Int
            ),

            "Q2.3.4" -> Map(
                "CosineSimilarityStatistics" -> Map(
                "min" -> minMult._1,  // Datatype of answer: Double
                "max" -> minMult._2, // Datatype of answer: Double
                "average" -> minMult._3, // Datatype of answer: Double
                "stddev" -> minMult._4 // Datatype of answer: Double
                )
            ),

            "Q2.3.5" -> Map(
                // Provide the formula that computes the amount of memory for storing all S(u,v)
                // as a function of U in the report.
                "TotalBytesToStoreNonZeroSimilarityComputationsForU1BaseDataset" -> nbBytes // Datatype of answer: Int
            ),

            "Q2.3.6" -> Map(
                "DurationInMicrosecForComputingPredictions" -> Map(
                "min" ->predictionMeasures._1,  // Datatype of answer: Double
                "max" ->predictionMeasures._2, // Datatype of answer: Double
                "average" ->predictionMeasures._3, // Datatype of answer: Double
                "stddev" ->predictionMeasures._4 // Datatype of answer: Double
                )
                // Discuss about the time difference between the similarity method and the methods
                // from milestone 1 in the report.
            ),

            "Q2.3.7" -> Map(
                "DurationInMicrosecForComputingSimilarities" -> Map(
                "min" -> similarityMeasures._1,  // Datatype of answer: Double
                "max" -> similarityMeasures._2, // Datatype of answer: Double
                "average" -> similarityMeasures._3, // Datatype of answer: Double
                "stddev" -> similarityMeasures._4 // Datatype of answer: Double
                ),
                "AverageTimeInMicrosecPerSuv" -> (similarityMeasures._3 / nbOfSuv.toDouble), // Datatype of answer: Double
                "RatioBetweenTimeToComputeSimilarityOverTimeToPredict" -> (similarityMeasures._3 / predictionMeasures._3.toDouble) // Datatype of answer: Double
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
