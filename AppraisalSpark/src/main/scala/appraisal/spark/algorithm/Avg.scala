package appraisal.spark.algorithm

import appraisal.spark.entities._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import appraisal.spark.util.Util
import appraisal.spark.interfaces.ImputationAlgorithm
import scala.collection.mutable.HashMap
import org.apache.spark.broadcast._
import appraisal.spark.statistic.Statistic

class Avg extends ImputationAlgorithm {
  
  def run(idf: DataFrame, cdf:DataFrame = null, params: HashMap[String, Any] = null): Entities.ImputationResult = {
    
    val attribute: String = params("imputationFeature").asInstanceOf[String] 
    
    val fidf = idf
    
    val columns = fidf.columns
      
    val avgidf = Util.filterNullAndNonNumericByAtt(fidf, columns.indexOf(attribute))
    avgidf.createOrReplaceTempView("originaldb")
    
    val avgValue = avgidf.sqlContext.sql("select avg(" + attribute + ") from originaldb").head().getAs[Double](0)
    
    val rdf = fidf.withColumn("imputationValue", when(col(attribute).isNotNull, col(attribute)).otherwise(avgValue))
    
    rdf.createOrReplaceTempView("result")
    val result = rdf.sqlContext.sql("select lineId, originalValue, imputationValue from result where " + attribute + " is null").rdd
    
    val impResult = Entities.ImputationResult(result.map(r => {
      
      val lineId = r.getLong(0)
      val originalValue = r.getDouble(1)
      val imputationValue = r.getDouble(2)
      
      Entities.Result(lineId, originalValue, imputationValue)}), 0, 0, 0, 0, 0,0, null ,params.toString())
      
    Statistic.statisticInfo(impResult)
    
  }
  
  def name(): String = {"Avg"}
  
}