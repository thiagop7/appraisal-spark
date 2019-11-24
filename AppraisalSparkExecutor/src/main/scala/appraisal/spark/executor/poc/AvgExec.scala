package appraisal.spark.executor.poc

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import appraisal.spark.executor.util.Util
import appraisal.spark.eraser.Eraser
import appraisal.spark.entities._
import appraisal.spark.algorithm._
import appraisal.spark.statistic._
import org.apache.spark.sql.functions._
import scala.collection.mutable.HashMap

object AvgExec {
  
  def main(args: Array[String]) {
    
    try{
      
      Logger.getLogger("org").setLevel(Level.ERROR)
      
      val spark = SparkSession
        .builder
        .appName("LinearRegressionDF")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
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
          "mitoses",
          "class")
          
      val feature = features(1)
        
      val odf = Util.loadBreastCancer(spark).withColumn("lineId", monotonically_increasing_id)
                                      .withColumn("originalValue", appraisal.spark.util.Util.toDouble(col(feature)))
      
      val percent = (10, 20, 30, 40, 50)
      
      val idf = new Eraser().run(odf, feature, percent._1)
      
      val params: HashMap[String, Any] = HashMap(
          "imputationFeature" -> feature)
      
      val imputationResult = new Avg().run(idf, params)
      
      imputationResult.result.foreach(Logger.getLogger("appraisal").error(_))
      
      Logger.getLogger("appraisal").error("totalError: " + imputationResult.totalError)
      Logger.getLogger("appraisal").error("avgError: " + imputationResult.avgError)
      Logger.getLogger("appraisal").error("avgPercentError: " + imputationResult.avgPercentError)
      
    }catch{
      
      case ex : Throwable => Logger.getLogger("appraisal").error(ex)
      
    }
    
  }
  
}