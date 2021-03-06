package appraisal.spark.statistic

import appraisal.spark.entities._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import appraisal.spark.util.Util
import java.text.DecimalFormat
import org.apache.spark.broadcast._

object Statistic {
  
  def statisticInfo(impres :Entities.ImputationResult) :Entities.ImputationResult = {
    
    val impResult = Entities.ImputationResult(impres.result.map(r => {
      
      val error = scala.math.sqrt(scala.math.pow((r.imputationValue - r.originalValue), 2))
      val percentualError = ((error / r.originalValue) * 100)      
      
      Entities.Result(r.lineId, r.originalValue, r.imputationValue, error, percentualError)
      
    }))
    
    val count = impResult.result.count()
    val totalError = impres.totalError//impResult.result.map(_.error).reduce(_ + _)
    val avgError = impres.avgError //(totalError / count).doubleValue()
    val avgPercentualError = impres.avgPercentError //(impResult.result.map(_.percentualError).reduce(_ + _) / count).doubleValue()
    val varianceImputedError = impres.varianceImputedError
    val varianteCompletedError = impres.varianceCompleteError
    val weights = impres.params
    
    Entities.ImputationResult(impResult.result, impres.k, avgError, totalError, avgPercentualError, varianteCompletedError, varianceImputedError, impres.boostedparams, impres.params)
    
  }
  
  
  def statisticBoostedInfo(impres : Entities.ImputationResult) :Entities.ImputationResult = {
    
    val impResult = Entities.ImputationResult(impres.result.map(r => {
      
      val error = scala.math.sqrt(scala.math.pow((r.imputationValue - r.originalValue), 2))
      val percentualError = ((error / r.originalValue) * 100)      
      
      Entities.Result(r.lineId, r.originalValue, r.imputationValue, error, percentualError)
      
    }))
    
    val count = impResult.result.count()
    val totalError = impResult.result.map(_.error).reduce(_ + _)
    val avgError = (totalError / count).doubleValue()
    val avgPercentualError = (impResult.result.map(_.percentualError).reduce(_ + _) / count).doubleValue()
    val varianceImputedError = impres.varianceImputedError
    val varianteCompletedError = impres.varianceCompleteError
    
    Entities.ImputationResult(impResult.result, impres.k, avgError, totalError, avgPercentualError, varianteCompletedError, varianceImputedError, impres.boostedparams, impres.params)
  }
  
}