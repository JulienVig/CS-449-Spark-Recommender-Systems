import org.rogach.scallop._
import org.json4s.jackson.Serialization
import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer

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
      println("")
      println("******************************************************")
      
      var conf = new Conf(args)
      val n_users = conf.users()
      val n_movies = conf.movies()

    println("Loading training data from: " + conf.train())
    val read_start = System.nanoTime
    val trainFile = Source.fromFile(conf.train())
    val trainBuilder = new CSCMatrix.Builder[Double](rows=n_users, cols=n_movies) 
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
    val testBuilder = new CSCMatrix.Builder[Double](rows=n_users, cols=n_movies) 
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

    def compute_similarities(k:Int=200) = {
        val ones_movies = DenseVector.ones[Double](n_movies)
        val user_nz = DenseVector.zeros[Double](n_users)
        for ((k,v) <- train.activeIterator) user_nz(k._1) += 1
        val user_avg = (train * ones_movies) /:/ user_nz                         //r^bar_u
        
        val norm_dev_builder = new CSCMatrix.Builder[Double](rows=n_users, cols=n_movies) 
        for ((k,v) <- train.activeIterator) {
            val u_avg = user_avg(k._1)
            norm_dev_builder.add(k._1, k._2, (v - u_avg) / scale(v, u_avg))
        }
        val user_norm_dev = norm_dev_builder.result()               //r^hat_{u,i}
        
        val sum_sq_norm_dev = sqrt(pow(user_norm_dev, 2) * ones_movies)
        val r_cup_builder = new CSCMatrix.Builder[Double](rows=n_users, cols=n_movies) 
        for ((k,v) <- user_norm_dev.activeIterator) r_cup_builder.add(k._1, k._2, v / sum_sq_norm_dev(k._1))
        val r_cup = r_cup_builder.result()                  //r^cup_{u,i}
        
        val sim_builder = new CSCMatrix.Builder[Double](rows=n_users, cols=n_users) 
        for (u <- 0 until n_users){
            val u_vec = r_cup(u, 0 until n_movies).t.copy
            val prod = r_cup * u_vec
            prod(u) = 0
            for (v <- argtopk(prod, k)) sim_builder.add(u, v, prod(v))
        }
        val topk_sim = sim_builder.result()
        (topk_sim, user_norm_dev, user_avg)
    }

    def compute_predictions(k:Int=200) = {
        val start_sim = System.nanoTime 
        val (topk_sim, user_norm_dev, user_avg) = compute_similarities(k)
        val end_sim = System.nanoTime
        val start_pred = System.nanoTime

        val pred_builder = new CSCMatrix.Builder[Double](rows=n_users, cols=n_movies)
        var curr_i = -1
        var i_dev = DenseVector.zeros[Double](n_users)
        var i_dev_mask = DenseVector.zeros[Double](n_users)
        for (((u, i),v) <- test.activeIterator){
            if (i != curr_i){
                curr_i = i
                i_dev = user_norm_dev(0 until n_users, i).copy
                i_dev_mask = abs(signum(i_dev))
            }
            
            val u_avg = user_avg(u)
            val u_sim = topk_sim(u, 0 until n_users).t.copy
            val u_abs_sim = sum(abs(u_sim) * i_dev_mask) //Only sum sim where i is non zero
            if (u_abs_sim != 0) {
                val u_i_sum = sum(u_sim * i_dev)
                val ui_dev = u_i_sum / u_abs_sim 
                pred_builder.add(u, i, u_avg + ui_dev * scale(u_avg + ui_dev, u_avg))
            } else pred_builder.add(u, i, u_avg)
        }
        val pred = pred_builder.result()
        val end_pred = System.nanoTime
        (pred, (end_sim - start_sim) / 1000, (end_pred - start_pred) / 1000)
    } 

    def statistics(measurements: Seq[Long]) = {
        val min = measurements.min
        val max = measurements.max
        val average = measurements.sum / measurements.length
        val std = math.sqrt((measurements.map(_ - average).map(t => t*t).sum) / measurements.length)
        (min, max, average, std)
    }

    def compute_mae(pred: CSCMatrix[Double]) = sum(abs(pred-test))/test.activeSize
    def measure(func: () => (CSCMatrix[Double], Long, Long), n: Int = 5) = {
        val (preds, measureSimilarity, measurePredictions) = (1 to n).map(a => func()).unzip3
        (statistics(measureSimilarity), statistics(measurePredictions))
    }
    
    val (knnMeasures, predictionMeasures) = measure(() => compute_predictions(), n = 5)
    
    val (pred_200,_,_) = compute_predictions(200)
    val (pred_100,_,_) = compute_predictions(100)
    
    val mae_200 = compute_mae(pred_200)
    val mae_100 = compute_mae(pred_100)


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
            "Q3.3.1" -> Map(
              "MaeForK=100" -> mae_100, // Datatype of answer: Double
              "MaeForK=200" -> mae_200  // Datatype of answer: Double
            ),
            "Q3.3.2" ->  Map(
              "DurationInMicrosecForComputingKNN" -> Map(
                "min" -> knnMeasures._1,  // Datatype of answer: Double
                "max" -> knnMeasures._2, // Datatype of answer: Double
                "average" -> knnMeasures._3, // Datatype of answer: Double
                "stddev" -> knnMeasures._4 // Datatype of answer: Double
              )
            ),
            "Q3.3.3" ->  Map(
              "DurationInMicrosecForComputingPredictions" -> Map(
                "min" -> predictionMeasures._1,  // Datatype of answer: Double
                "max" -> predictionMeasures._2, // Datatype of answer: Double
                "average" -> predictionMeasures._3, // Datatype of answer: Double
                "stddev" -> predictionMeasures._4 // Datatype of answer: Double
              )
            )
            // Answer the Question 3.3.4 exclusively on the report.
           )
          json = Serialization.writePretty(answers)
        }

        println(json)
        println("Saving answers in: " + jsonFile)
        printToFile(json, jsonFile)
      }
    }

    println("")
  } 
}
