package appraisal.spark.interfaces

import org.apache.spark.sql._
import appraisal.spark.entities._
import scala.collection.mutable.HashMap
import org.apache.spark.broadcast._

trait ImputationAlgorithm extends AppraisalAlgorithm {
  
  def run(idf: DataFrame, cdf:DataFrame = null, params: HashMap[String, Any] = null): Entities.ImputationResult
  
}