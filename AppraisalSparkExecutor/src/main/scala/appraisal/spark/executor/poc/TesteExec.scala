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

object TesteExec {
  
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
      
      var _idf = idf
      var _odf = odf
      var _features = features
      
      val context = _idf.sparkSession.sparkContext
      
      val remCol = _odf.columns.diff(_features).filter(c => !"lineId".equals(c) && !feature.equals(c) && !"originalValue".equals(c))
      _odf = _odf.drop(remCol: _*)
      _odf = appraisal.spark.util.Util.filterNullAndNonNumeric(_odf)
      
      _odf.columns.filter(!"lineId".equals(_)).foreach(att => _odf = _odf.withColumn(att, appraisal.spark.util.Util.toDouble(col(att))))
     
      val calcCol = _features.filter(!_.equals(feature))
      val removeCol = _idf.columns.diff(_features).filter(c => !"lineId".equals(c) && !feature.equals(c) && !"originalValue".equals(c))
      
      var vnidf = appraisal.spark.util.Util.filterNullAndNonNumeric(_idf.drop(removeCol: _*), calcCol)
      
      val impcolindex = vnidf.columns.indexOf(feature)
      vnidf = vnidf.filter(r => appraisal.spark.util.Util.isNumericOrNull(r.get(impcolindex)))
      
      vnidf.columns.filter(!"lineId".equals(_)).foreach(att => vnidf = vnidf.withColumn(att, appraisal.spark.util.Util.toDouble(col(att))))
      var _vnidf = vnidf
      
      println("calcCol - ")
      calcCol.foreach(println(_))
      
      _vnidf.foreach(r => println(r.toString()))
      
    }catch{
      
      case ex : Throwable => Logger.getLogger("appraisal").error(ex)
      
    }
    
  }
  
}