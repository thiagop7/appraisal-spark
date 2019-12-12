package appraisal.spark.strategies

import org.apache.spark.sql._
import scala.collection.mutable.HashMap
import appraisal.spark.interfaces._
import appraisal.spark.entities.Entities
import org.apache.spark.broadcast._

class SelectionStrategy(var params: HashMap[String, Any] = null, var selectionAlgorithm: SelectionAlgorithm) extends AppraisalStrategy {
  
  def run(idf: DataFrame, cdf: DataFrame = null): Entities.SelectionResult = {
    selectionAlgorithm.run(idf, cdf, params)
  }
  
  def algName(): String = {
    selectionAlgorithm.name()
  }
  
  def strategyName: String = {"Selection"}
  
  def parameters: String = {params.toString()}
  
}