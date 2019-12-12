package appraisal.spark.strategies

import org.apache.spark.sql._
import scala.collection.mutable.HashMap
import appraisal.spark.interfaces._
import appraisal.spark.entities.Entities
import org.apache.spark.broadcast._

class ImputationStrategy(var params: HashMap[String, Any] = null, var imputationAlgorithm: ImputationAlgorithm) extends AppraisalStrategy {
  
  def run(idf: DataFrame, cdf: DataFrame = null): Entities.ImputationResult = {
    imputationAlgorithm.run(idf, cdf, params)
  }
  
  def algName(): String = {
    imputationAlgorithm.name()
  }
  
  def strategyName: String = {"Imputation"}
  
  def parameters: String = {params.toString()}
  
}