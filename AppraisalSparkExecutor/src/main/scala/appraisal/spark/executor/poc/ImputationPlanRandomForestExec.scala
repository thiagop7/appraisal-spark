package appraisal.spark.executor.poc

import org.apache.log4j._
import appraisal.spark.engine.ImputationPlan
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import appraisal.spark.algorithm.Knn
import appraisal.spark.eraser.Eraser
import appraisal.spark.executor.util.Util
import org.apache.spark.sql.functions._
import scala.collection.mutable.HashMap
import appraisal.spark.strategies._
import appraisal.spark.algorithm._
import appraisal.spark.engine.Crowner
import appraisal.spark.engine.Reviewer

object ImputationPlanRandomForestExec extends Serializable {

  def main(args: Array[String]) {

    var parallelExecution = true
    var breastCancer = true
    var aidsOccurrence = false

    if (args != null && args.length > 0) {

      if ("single".equalsIgnoreCase(args(0))) {

        parallelExecution = false

      } else if ("breastcancer".equalsIgnoreCase(args(0))) {

        breastCancer = true

      } else if ("aidsoccurrence".equalsIgnoreCase(args(0))) {

        aidsOccurrence = true

      }

      if (args.length > 1) {

        if ("breastcancer".equalsIgnoreCase(args(1)))
          breastCancer = true

        else if ("aidsoccurrence".equalsIgnoreCase(args(1)))
          aidsOccurrence = true

      }
    }

    parallelExecution = false

    val wallStartTime = new java.util.Date()
    Logger.getLogger(getClass.getName).error("Appraisal Spark - Wall start time: " + appraisal.spark.util.Util.getCurrentTime(wallStartTime))
    Logger.getLogger(getClass.getName).error("Parallel execution: " + parallelExecution)

    try {

      val conf = new SparkConf()
        //.set("spark.executor.memory", "1g")
        //.set("spark.executor.cores", "8")
        .set("spark.network.timeout", "10800")
        .set("spark.sql.broadcastTimeout", "10800")
        .set("spark.executor.extraJavaOptions", "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps")
        .set("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.

      val spark = SparkSession
        .builder
        .appName("AppraisalSpark")
        .master("local[*]")
        //.master("spark://127.0.0.1:7077")
        .config(conf)
        .getOrCreate()

      var features: Array[String] = null
      var feature = ""

      var odf: DataFrame = null

      if (breastCancer) {

        odf = Util.loadData(spark, "file:///rukbat/appraisal/breast_cancer_wisconsin.csv").withColumn("lineId", monotonically_increasing_id)

        //odf = Util.loadBreastCancer(spark).withColumn("lineId", monotonically_increasing_id)
        features = Util.breastcancer_features

      } else if (aidsOccurrence) {

        odf = Util.loadData(spark, "file:///shared/appraisal/AIDS_Occurrence_and_Death_and_Queries.csv").withColumn("lineId", monotonically_increasing_id)
        features = Util.aidsocurrence_features

        //odf = Util.loadAidsOccurenceAndDeath(spark).withColumn("lineId", monotonically_increasing_id)
        //features = Util.aidsocurrence_features

      }

      Logger.getLogger(getClass.getName).error("Data count: " + odf.count())

      //val k_nsqrt = scala.math.sqrt(odf.value.count()).intValue()
      val kn = odf.count().intValue()

      var imputationPlans = List.empty[(String, Double, Double, Int, ImputationPlan)]

      //val missingRate = Seq(10d, 20d, 30d)
      val missingRate = Seq(20d)

      //val selectionReduction = Seq(10d, 20d, 30d)
      val selectionReduction = Seq(10d)

      val T = 3;

      features.foreach(feat => {

        feature = feat
        odf = odf.withColumn("originalValue", col(feature))

        //Cálculo da variância por feature para comparação depois de imputado
        val arrFeatComplete = odf.select(col(feature)).collect().map(_.toSeq.toArray).flatten
        val varianceBefore = Util.variance(arrFeatComplete.map(x => Util.extractDouble(x)))

        missingRate.foreach(mr => {

          //val idf = new Eraser().run(odf, feature, mr)
          val idf = null

          selectionReduction.foreach(sr => {

            var impPlan = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)

            var selectionParams: HashMap[String, Any] = HashMap(
              "percentReduction" -> sr)

            var clusteringParams: HashMap[String, Any] = HashMap(
              "k" -> 4,
              "maxIter" -> 1000,
              "kLimit" -> 10)

            var imputationParams: HashMap[String, Any] = HashMap(
              "k" -> 5,
              "kLimit" -> 10,
              "varianceComplete" -> varianceBefore.get,
              "learningRate" -> 0.1,
              "features" -> features)

            var adaboostParams: HashMap[String, Any] = HashMap(
              "imputationPlan" -> impPlan)

            // regression

            // impPlan.addStrategy(new ImputationStrategy(imputationParams, new BaggingReg()))

            // impPlan.addEnsembleStrategy(new EnsembleStrategy(adaboostParams, new Boost()))

            // imputationPlans = imputationPlans :+ (feature, mr, sr, T, impPlan)

            // clustering -> regression

            impPlan.addStrategy(new ClusteringStrategy(clusteringParams, new KMeans()))

            impPlan.addStrategy(new ImputationStrategy(imputationParams, new BaggingReg()))

            impPlan.addEnsembleStrategy(new EnsembleStrategy(adaboostParams, new Boost()))

            imputationPlans = imputationPlans :+ (feature, mr, sr, T, impPlan)
          })

        })

      })

      var resultList = new Crowner().runEnsemle(imputationPlans, parallelExecution)

      var consResult = new Reviewer().runEnsemble(resultList, missingRate, selectionReduction)

      Logger.getLogger(getClass.getName).error("")
      Logger.getLogger(getClass.getName).error("")
      Logger.getLogger(getClass.getName).error("")
      Logger.getLogger(getClass.getName).error(" ------ CONSOLIDATED RESULT ------ ")

      consResult.foreach(x => {

        Logger.getLogger(getClass.getName).error("Plan: " + x._1 + "	" + "Missing rate: " + x._2 + "	"
          + "Selection reduction: " + x._3 + "	" + "Error: " + x._4 + "%")

      })

      val bestPlan = consResult.sortBy(_._4).head

      Logger.getLogger(getClass.getName).error("")
      Logger.getLogger(getClass.getName).error("Best plan: " + bestPlan._1 + "	" + "Missing rate: " + bestPlan._2 + "	"
        + "Selection reduction: " + bestPlan._3 + "	" + "Error: " + bestPlan._4 + "%")

      val wallStopTime = new java.util.Date()

      val wallTimeseconds = ((wallStopTime.getTime - wallStartTime.getTime) / 1000)

      val wallTimesMinutes = wallTimeseconds / 60

      val wallTimesHours = wallTimesMinutes / 60

      Logger.getLogger(getClass.getName).error("------------------ Wall stop time: "
        + appraisal.spark.util.Util.getCurrentTime(wallStartTime)
        + " --- Total wall time: " + wallTimeseconds + " seconds, "
        + wallTimesMinutes + " minutes, "
        + wallTimesHours + " hours.")

    } catch {

      case ex: Exception => {
        ex.printStackTrace()
        Logger.getLogger(getClass.getName).error(ex)
      }

    }

  }

}