package appraisal.spark.executor.poc

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import appraisal.spark.executor.util.Util
import appraisal.spark.algorithm.Pca
import appraisal.spark.eraser.Eraser
import scala.collection.mutable.HashMap
import org.apache.spark.sql.functions._

object PcaExec {
  
  def main(args: Array[String]) {
    
    try{
      
      Logger.getLogger("org").setLevel(Level.ERROR)
      
      val spark = SparkSession
        .builder
        .appName("LinearRegressionDF")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
        .getOrCreate()
      
      val features = appraisal.spark.executor.util.Util.breastcancer_features
      //val features = appraisal.spark.executor.util.Util.aidsocurrence_features
          
      val feature = features(2)
        
      val odf = Util.loadBreastCancer(spark).withColumn("lineId", monotonically_increasing_id)
                                            .withColumn("originalValue", col(feature))
      
      val percentReduction = (10d, 20d, 30d, 40d, 50d)
      
      // --- Validacao da ImputationPlan --- //
      
      val calcCol = features.filter(!_.equals(feature))
      val removeCol = odf.columns.diff(features).filter(c => !"lineId".equals(c) && !feature.equals(c) && !"originalValue".equals(c))
      var vnidf = appraisal.spark.util.Util.filterNullAndNonNumeric(odf.drop(removeCol: _*), calcCol)
      vnidf.columns.filter(!"lineId".equals(_)).foreach(att => vnidf = vnidf.withColumn(att, appraisal.spark.util.Util.toDouble(col(att))))
      val _vnidf = vnidf
      
      // ----------------------------------- //
      
      val params: HashMap[String, Any] = HashMap(
          "imputationFeature" -> feature,
          //"percentReduction" -> percentReduction._1)
          "percentReduction" -> 0d)
      
      val res = new Pca().run(_vnidf, params)
      
      res.result.sortBy(_.index).collect().foreach(Logger.getLogger("appraisal").error(_))
      
    }catch{
      
      case ex : Throwable => Logger.getLogger("appraisal").error(ex)
      
    }
    
  }
  
}