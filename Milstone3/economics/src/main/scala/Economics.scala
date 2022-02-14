import org.rogach.scallop._
import org.json4s.jackson.Serialization
import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val json = opt[String]()
  verify()
}

object Economics {
  def main(args: Array[String]) {
    println("")
    println("******************************************************")

    var conf = new Conf(args)

    val iccPrice = 35000
    val iccDailyRent = 20.4
    val iccNbCore = 2 * 14
    val iccRAM = 24 * 64
    val minDaysRentingICC = ceil(iccPrice / iccDailyRent)
    val minYearsRentingICC = minDaysRentingICC / 365

    val containerCoreRent = 0.092
    val containerRAMRent = 0.012
    val dailyCostICContainer_Eq_ICC = iccNbCore * containerCoreRent + iccRAM * containerRAMRent
    val ratioICC = iccDailyRent / dailyCostICContainer_Eq_ICC
    val containerCheaperThanICC = ratioICC > 1.05

    val minPowerCost = 0.0108
    val maxPowerCost = 0.054
    val rpiRAM = 8
    val dailyCostICContainer_Eq_4RPi4_Throughput = containerCoreRent + 4 * rpiRAM * containerRAMRent
    val ratio4RPi_over_Container_MaxPower = 4 * maxPowerCost / dailyCostICContainer_Eq_4RPi4_Throughput
    val ratio4RPi_over_Container_MinPower = 4 * minPowerCost / dailyCostICContainer_Eq_4RPi4_Throughput
    val containerCheaperThan4RPi = ratio4RPi_over_Container_MaxPower > 1.05

    val rpiPrice = 94.83
    val minDaysRentingContainerToPay4RPis_MinPower = ceil(4 * rpiPrice / (dailyCostICContainer_Eq_4RPi4_Throughput - 4 * minPowerCost))
    val minDaysRentingContainerToPay4RPis_MaxPower = ceil(4 * rpiPrice / (dailyCostICContainer_Eq_4RPi4_Throughput - 4 * maxPowerCost))

    val nbRPisForSamePriceAsICC = floor(iccPrice / rpiPrice)
    val ratioTotalThroughputRPis_over_ThroughputICC = nbRPisForSamePriceAsICC / (4 * iccNbCore)
    val ratioTotalRAMRPis_over_RAMICC = rpiRAM * nbRPisForSamePriceAsICC / iccRAM

    val nbUserPerGB = pow(10,9) / (2.0*900)
    val nbUserPerRPi = rpiRAM * nbUserPerGB
    val nbUserPerICC = iccRAM * nbUserPerGB
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
            "Q5.1.1" -> Map(
              "MinDaysOfRentingICC.M7" -> minDaysRentingICC, // Datatype of answer: Double
              "MinYearsOfRentingICC.M7" -> minYearsRentingICC // Datatype of answer: Double
            ),
            "Q5.1.2" -> Map(
              "DailyCostICContainer_Eq_ICC.M7_RAM_Throughput" -> dailyCostICContainer_Eq_ICC, // Datatype of answer: Double
              "RatioICC.M7_over_Container" -> ratioICC, // Datatype of answer: Double
              "ContainerCheaperThanICC.M7" -> containerCheaperThanICC // Datatype of answer: Boolean
            ),
            "Q5.1.3" -> Map(
              "DailyCostICContainer_Eq_4RPi4_Throughput" -> dailyCostICContainer_Eq_4RPi4_Throughput, // Datatype of answer: Double
              "Ratio4RPi_over_Container_MaxPower" -> ratio4RPi_over_Container_MaxPower, // Datatype of answer: Double
              "Ratio4RPi_over_Container_MinPower" -> ratio4RPi_over_Container_MinPower, // Datatype of answer: Double
              "ContainerCheaperThan4RPi" -> containerCheaperThan4RPi // Datatype of answer: Boolean
            ),
            "Q5.1.4" -> Map(
              "MinDaysRentingContainerToPay4RPis_MinPower" -> minDaysRentingContainerToPay4RPis_MinPower, // Datatype of answer: Double
              "MinDaysRentingContainerToPay4RPis_MaxPower" -> minDaysRentingContainerToPay4RPis_MaxPower // Datatype of answer: Double
            ),
            "Q5.1.5" -> Map(
              "NbRPisForSamePriceAsICC.M7" -> nbRPisForSamePriceAsICC, // Datatype of answer: Double
              "RatioTotalThroughputRPis_over_ThroughputICC.M7" -> ratioTotalThroughputRPis_over_ThroughputICC, // Datatype of answer: Double
              "RatioTotalRAMRPis_over_RAMICC.M7" -> ratioTotalRAMRPis_over_RAMICC // Datatype of answer: Double
            ),
            "Q5.1.6" ->  Map(
              "NbUserPerGB" -> nbUserPerGB, // Datatype of answer: Double 
              "NbUserPerRPi" -> nbUserPerRPi, // Datatype of answer: Double 
              "NbUserPerICC.M7" -> nbUserPerICC // Datatype of answer: Double 
            )
            // Answer the Question 5.1.7 exclusively on the report.
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
