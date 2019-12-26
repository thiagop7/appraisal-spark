package appraisal.spark.entities

import org.apache.spark.rdd.RDD
import appraisal.spark.engine.ImputationPlan
import scala.collection.mutable.HashMap
import appraisal.spark.interfaces.StrategyResult

object Entities {

  final case class Result(lineId: Long, originalValue: Double, imputationValue: Double,
                          error: Double = 0, percentualError: Double = 0)

  final case class ImputationResult(result: RDD[Result] = null, k: Int = 0, avgError: Double = 0,
                                    totalError: Double = 0, avgPercentError: Double = 0, varianceCompleteError: Double = 0, varianceImputedError: Double = 0, boostedparams: Seq[(Int, List[Double])] = null, params: String = null) extends StrategyResult

  final case class CResult(cluster: Int, lineId: Long)

  final case class ClusteringResult(result: RDD[CResult], k: Int = 0, wssse: Double = 0) extends StrategyResult

  final case class SResult(index: Int, attribute: String, col: Int, covariance: Double)

  final case class SelectionResult(result: RDD[SResult]) extends StrategyResult

  final case class EResult(imputationPlan: ImputationPlan, imputationResult: ImputationResult, voting: Long)

  final case class EnsembleResult(result: RDD[EResult])
  
  final case class Features(features: Seq[Any])

}