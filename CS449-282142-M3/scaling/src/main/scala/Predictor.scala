import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.log4j.Logger
import org.apache.log4j.Level
import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val k = opt[Int]()
  val json = opt[String]()
  val users = opt[Int]()
  val movies = opt[Int]()
  val separator = opt[String]()
  verify()
}

object Predictor {
  def main(args: Array[String]) {
    var conf = new Conf(args)
    // conf object is not serializable, extract values that
    // will be serialized with the parallelize implementations
    val conf_users = conf.users()
    val conf_movies = conf.movies()
    val conf_k = conf.k()

    // Remove these lines if encountering/debugging Spark
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    println("Loading training data from: " + conf.train())
    val read_start = System.nanoTime
    val trainFile = Source.fromFile(conf.train())
    val trainBuilder = new CSCMatrix.Builder[Double](rows=conf_users, cols=conf_movies) 
    for (line <- trainFile.getLines) {
        val cols = line.split(conf.separator()).map(_.trim)
        trainBuilder.add(cols(0).toInt-1, cols(1).toInt-1, cols(2).toDouble)
    }
    val train = trainBuilder.result()
    trainFile.close
    val read_duration = System.nanoTime - read_start
    println("Read data in " + (read_duration/pow(10.0,9)) + "s")

    println("Compute kNN on train data...")
    
    println("Loading test data from: " + conf.test())
    val testFile = Source.fromFile(conf.test())
    val testBuilder = new CSCMatrix.Builder[Double](rows=conf_users, cols=conf_movies) 
    for (line <- testFile.getLines) {
        val cols = line.split(conf.separator()).map(_.trim)
        testBuilder.add(cols(0).toInt-1, cols(1).toInt-1, cols(2).toDouble)
    }
    val test = testBuilder.result()
    testFile.close

    println("Compute predictions on test data...")

   def scale(rating:Double, avgUserRating:Double) = rating match {
        case x if x > avgUserRating => 5 - avgUserRating
        case x if x < avgUserRating => avgUserRating - 1
        case _ => 1
    }

    def preprocess_ratings() = {
        val ones_movies = DenseVector.ones[Double](conf_movies)
        val user_nz = DenseVector.zeros[Double](conf_users)
        for ((k,v) <- train.activeIterator) user_nz(k._1) += 1
        val user_avg = (train * ones_movies) /:/ user_nz            //r^bar_u
        
        val norm_dev_builder = new CSCMatrix.Builder[Double](rows=conf_users, cols=conf_movies) 
        for ((k,v) <- train.activeIterator) {
            val u_avg = user_avg(k._1)
            norm_dev_builder.add(k._1, k._2, (v - u_avg) / scale(v, u_avg))
        }
        val user_norm_dev = norm_dev_builder.result()               //r^hat_{u,i}
        
        val sum_sq_norm_dev = sqrt(pow(user_norm_dev, 2) * ones_movies)
        val preprocessed_builder = new CSCMatrix.Builder[Double](rows=conf_users, cols=conf_movies) 
        for ((k,v) <- user_norm_dev.activeIterator) preprocessed_builder.add(k._1, k._2, v / sum_sq_norm_dev(k._1))
        val preprocessed = preprocessed_builder.result()                  //r^cup_{u,i}
        (preprocessed, user_norm_dev, user_avg)
    }

    val start_time = System.nanoTime
    val (preprocessed, user_norm_dev, user_avg) = preprocess_ratings()

    val preprocessed_br = sc.broadcast(preprocessed)

    def topk(u:Int) = {
        val preprocessed = preprocessed_br.value
        val user_sim = preprocessed * preprocessed(u, 0 until conf_movies).t.copy   //s_{u,v}
        user_sim(u) = 0
        val topk_sim = argtopk(user_sim, conf_k).map(v => (v, user_sim(v)))
        (u, topk_sim)
    }
    val topks = sc.parallelize(0 until conf_users).map(topk).collect()

    val topk_sim_builder = new CSCMatrix.Builder[Double](rows=conf_users, cols=conf_users) 
    for ((u, u_sims) <- topks) for ((v, sim) <- u_sims) topk_sim_builder.add(u, v, sim)
    val topk_sim = topk_sim_builder.result()   
    val mid_time = System.nanoTime

    val u_avg_br = sc.broadcast(user_avg)
    val topk_sim_br = sc.broadcast(topk_sim)
    val user_norm_dev_br = sc.broadcast(user_norm_dev)

    def predict_user_item(u:Int, i:Int) = {
        val user_avg = u_avg_br.value
        val topk_sim = topk_sim_br.value
        val user_norm_dev = user_norm_dev_br.value

        val u_avg = user_avg(u)
        val u_sim = topk_sim(u, 0 until conf_users).t.copy
        val i_dev = user_norm_dev(0 until conf_users, i).copy

        val u_i_sum = sum(u_sim * i_dev)
        val u_abs_sim = sum(abs(u_sim) * abs(signum(i_dev))) //Only sum sim where i is non zero
        val ui_dev = if (u_abs_sim != 0) u_i_sum / u_abs_sim else 0
        (u, (i, u_avg + ui_dev * scale(u_avg + ui_dev, u_avg)))
    }
    val ui_test = (for (((u,i), v) <- test.activeIterator) yield (u,i)).toList
    val pred_rdd = sc.parallelize(ui_test).map({case (u,i) => predict_user_item(u,i)}).collect()

    val pred_builder = new CSCMatrix.Builder[Double](rows=conf_users, cols=conf_movies) 
    for ((u, (i, ui_pred)) <- pred_rdd) pred_builder.add(u, i, ui_pred)
    val pred = pred_builder.result()
    val end_time = System.nanoTime
    
    val knn_time = (mid_time - start_time) / 1000
    val pred_time = (end_time - mid_time) / 1000

    def compute_mae(pred: CSCMatrix[Double]) = sum(abs(pred - test)) / test.activeSize

    val mae = compute_mae(pred)

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
            "Q4.1.1" -> Map(
              "MaeForK=200" -> mae  // Datatype of answer: Double
            ),
            // Both Q4.1.2 and Q4.1.3 should provide measurement only for a single run
            "Q4.1.2" ->  Map(
              "DurationInMicrosecForComputingKNN" -> knn_time  // Datatype of answer: Double
            ),
            "Q4.1.3" ->  Map(
              "DurationInMicrosecForComputingPredictions" -> pred_time // Datatype of answer: Double  
            )
            // Answer the other questions of 4.1.2 and 4.1.3 in your report
           )
          json = Serialization.writePretty(answers)
        }

        println(json)
        println("Saving answers in: " + jsonFile)
        printToFile(json, jsonFile)
      }
    }

    println("")
    spark.stop()
  } 
}
