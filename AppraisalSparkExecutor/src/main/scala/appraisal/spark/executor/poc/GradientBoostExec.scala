package appraisal.spark.executor.poc

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import appraisal.spark.algorithm.GradientBoost
import appraisal.spark.eraser.Eraser
import appraisal.spark.executor.util.Util
import appraisal.spark.statistic._
import org.apache.spark.sql.functions._
import scala.collection.mutable.HashMap


object GradientBoostExec {
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

    parallelExecution = true

    val wallStartTime = new java.util.Date()
    Logger.getLogger(getClass.getName).error("Appraisal Spark - Wall start time: " + appraisal.spark.util.Util.getCurrentTime())

    try {

      Logger.getLogger("org").setLevel(Level.ERROR)

      val conf = new SparkConf()
        //.set("spark.executor.memory", "1g")
        .set("spark.executor.cores", "4")
        .set("spark.network.timeout", "36000")
        .set("spark.sql.broadcastTimeout", "36000")
        .set("spark.executor.extraJavaOptions", "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps")
        .set("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.

      val spark = SparkSession
        .builder
        .appName("RandomForesDF")
        .master("local[*]")
        .config(conf) // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
        .getOrCreate()

      val features = Array[String](
        //"code_number",
        "clump_thickness",
        "uniformity_of_cell_size",
        "uniformity_of_cell_shape",
        "marginal_adhesion",
        "single_epithelial_cell_size",
        "bare_nuclei",
        "bland_chromatin",
        "normal_nucleoli",
        "mitoses")
      //"class")

      val feature = features(1)

      var odf: DataFrame = null

      if (breastCancer) {

        odf = Util.loadData(spark, "file:///home/oem/Workspace/mestrado/appraisalAdaboost-spark/data/breast-cancer-wisconsin.reduced.csv").withColumn("lineId", monotonically_increasing_id)

      } else if (aidsOccurrence) {

        odf = Util.loadData(spark, "file:///shared/appraisal/AIDS_Occurrence_and_Death_and_Queries.csv").withColumn("lineId", monotonically_increasing_id)
      }
      
      
      val percent = (10, 20, 30, 40, 50)

      val idf = new Eraser().run(odf, feature, percent._1)

      // --- Validacao da ImputationPlan --- //

      val calcCol = features.filter(!_.equals(feature))
      val removeCol = idf.columns.diff(features).filter(c => !"lineId".equals(c) && !feature.equals(c) && !"originalValue".equals(c))
      var vnidf = appraisal.spark.util.Util.filterNullAndNonNumeric(idf.drop(removeCol: _*), calcCol)
      vnidf.columns.filter(!"lineId".equals(_)).foreach(att => vnidf = vnidf.withColumn(att, appraisal.spark.util.Util.toDouble(col(att))))
      val _vnidf = vnidf
      
      val removeColodf = odf.columns.diff(features).filter(c =>  !"lineId".equals(c) && !feature.equals(c) && !"originalValue".equals(c))
      var vnodf = appraisal.spark.util.Util.filterNullAndNonNumeric(odf.drop(removeColodf: _*), calcCol)
      vnodf.columns.filter(!"lineId".equals(_)).foreach(att => vnodf = vnodf.withColumn(att, appraisal.spark.util.Util.toDouble(col(att))))
      val _vnodf = vnodf
            
      // ----------------------------------- //

      //Cálculo da variância por feature para comparação depois de imputado
      val arrFeatComplete = odf.select(col(feature)).collect().map(_.toSeq.toArray).flatten
      val varianceBefore = Util.variance(arrFeatComplete.map(x => x.toString().toDouble)).get

      //val varianceBefore = Util.variance(arrFeatComplete.map(x => Util.extractDouble(x)))

      val params: HashMap[String, Any] = HashMap(
        "imputationFeature" -> feature,
        "calcFeatures" -> calcCol,
        "varianceComplete" -> varianceBefore,
        "learningRate" -> 0.1)

      val imputationResult = new GradientBoost().run(_vnidf, _vnodf, params)

      imputationResult.result.foreach(Logger.getLogger("appraisal").error(_))

      Logger.getLogger("appraisal").error("best k: " + imputationResult.k)
      Logger.getLogger("appraisal").error("totalError: " + imputationResult.totalError)
      Logger.getLogger("appraisal").error("avgError: " + imputationResult.avgError)
      Logger.getLogger("appraisal").error("avgPercentError: " + imputationResult.avgPercentError)
      //Logger.getLogger("appraisal").error("varianceImputedError: " + imputationResult.varianceImputedError)
      Logger.getLogger("appraisal").error("varianceCompleteError: " + imputationResult.varianceCompleteError)

      val wallStopTime = new java.util.Date()

      val wallTimeseconds = ((wallStopTime.getTime - wallStartTime.getTime) / 1000)

      val wallTimesMinutes = wallTimeseconds / 60

      val wallTimesHours = wallTimesMinutes / 60

      Logger.getLogger(getClass.getName).error("------------------ Wall stop time: "
        + appraisal.spark.util.Util.getCurrentTime()
        + " --- Total wall time: " + wallTimeseconds + " seconds, "
        + wallTimesMinutes + " minutes, "
        + wallTimesHours + " hours.")

    } catch {

      case ex: Exception => Logger.getLogger("appraisal").error(ex)

    }

  }
}