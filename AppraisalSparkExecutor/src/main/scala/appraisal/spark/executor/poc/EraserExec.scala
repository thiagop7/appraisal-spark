package appraisal.spark.executor.poc

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import appraisal.spark.executor.util.Util
import appraisal.spark.eraser.Eraser

object EraserExec {
  
  def main(args: Array[String]) {
    
    try{
    
      Logger.getLogger("org").setLevel(Level.ERROR)
      
      val spark = SparkSession
        .builder
        .appName("LinearRegressionDF")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
        .getOrCreate()
      
      var df = Util.loadBreastCancer(spark)
      
      val percent = (10, 20, 30, 40, 50)
      
      val features = (//"code_number",
          "clump_thickness","uniformity_of_cell_size","uniformity_of_cell_shape","marginal_adhesion","single_epithelial_cell_size","bare_nuclei","bland_chromatin","normal_nucleoli",
          "mitoses")
          //,"class")
      
      new Eraser().run(df, features._1, percent._1)
    
    }catch{
      
      case ex : Throwable => Logger.getLogger("appraisal").error(ex)
      
    }
    
  }
  
}