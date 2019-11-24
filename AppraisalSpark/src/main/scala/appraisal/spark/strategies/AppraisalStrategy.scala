package appraisal.spark.strategies
import org.apache.spark.sql._
import appraisal.spark.entities._
import org.apache.spark.broadcast._
import appraisal.spark.interfaces.StrategyResult

trait AppraisalStrategy extends Serializable {
  
  def run(idf: DataFrame): StrategyResult
  
  def algName(): String
  
  def strategyName: String
  
  def parameters: String
  
}