package appraisal.spark.strategies

import scala.collection.mutable.HashMap
import appraisal.spark.interfaces._
import appraisal.spark.entities.Entities
import org.apache.spark.sql._

class EnsembleStrategy(var params: HashMap[String, Any] = null, var ensembleAlgorithm: EnsembleAlgorithm) extends AppraisalStrategy {
  
  def run(idf: DataFrame): Entities.ImputationResult = {
    ensembleAlgorithm.run(idf, params)
  }
  
  def algName(): String = {
    ensembleAlgorithm.name()
  }
  
  def strategyName: String = {"Ensemble"}
  
  def parameters: String = {params.toString()}
  
}