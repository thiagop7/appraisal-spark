package appraisal.spark.algorithm

import org.apache.spark.sql._
import appraisal.spark.entities._
import scala.collection.mutable.HashMap
import appraisal.spark.interfaces.ClusteringAlgorithm
import org.apache.spark.broadcast._

class KMeansPlus extends ClusteringAlgorithm {

  def run(idf: DataFrame, cdf: DataFrame = null, params: HashMap[String, Any] = null): Entities.ClusteringResult = {

    val k: Int = params("k").asInstanceOf[Int]
    val kLimit: Int = params("kLimit").asInstanceOf[Int]
    val maxIter: Int = params("maxIter").asInstanceOf[Int]

    var clusteringResult: Entities.ClusteringResult = null

    var lastWssse: (Double, Entities.ClusteringResult) = (0d, null)

    var _k = k

    //for(_k <- k to kLimit){

    val kMax = idf.count() / 10
    while (_k <= kMax) {

      val _params: HashMap[String, Any] = params
      _params.update("k", _k)

      clusteringResult = new KMeans().run(idf, null, params)

      if (_k == k || clusteringResult.wssse < lastWssse._1) {

        lastWssse = (clusteringResult.wssse, clusteringResult)

      } else {

        println("KEscolhido: " + _k)
        return lastWssse._2

      }

      _k += 1

    }
    println("KEscolhido: " + _k)
    return lastWssse._2

  }

  def name(): String = { "KMeansPlus" }

}