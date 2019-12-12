package appraisal.spark.interfaces

import scala.collection.mutable.HashMap
import org.apache.spark.sql._
import org.apache.spark.broadcast._

trait AppraisalAlgorithm extends Serializable {
  
  def run(idf: DataFrame, cdf:DataFrame = null, params: HashMap[String, Any] = null): StrategyResult
  
  def name(): String
  
}