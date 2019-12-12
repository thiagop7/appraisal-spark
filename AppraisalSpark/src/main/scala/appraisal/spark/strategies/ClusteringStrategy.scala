package appraisal.spark.strategies

import org.apache.spark.sql._
import scala.collection.mutable.HashMap
import appraisal.spark.interfaces._
import appraisal.spark.entities.Entities
import org.apache.spark.broadcast._

class ClusteringStrategy(var params: HashMap[String, Any] = null, var clusteringAlgorithm: ClusteringAlgorithm) extends AppraisalStrategy {
  
  def run(idf: DataFrame, cdf:DataFrame = null): Entities.ClusteringResult = {
    clusteringAlgorithm.run(idf, cdf, params)
  }
  
  def algName(): String = {
    clusteringAlgorithm.name()
  }
  
  def strategyName: String = {"Clustering"}
  
  def parameters: String = {params.toString()}
  
}