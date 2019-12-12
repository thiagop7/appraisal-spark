package appraisal.spark.interfaces

import org.apache.spark.sql._
import appraisal.spark.entities._
import scala.collection.mutable.HashMap

trait EnsembleAlgorithm extends AppraisalAlgorithm {
  
  def run(idf: DataFrame, cdf:DataFrame = null, params: HashMap[String, Any] = null): Entities.ImputationResult
  
}