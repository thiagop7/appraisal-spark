package appraisal.spark.algorithm

import appraisal.spark.entities._
import org.apache.spark.sql._
import org.apache.spark.rdd._
import org.apache.spark.sql.types._
import org.apache.spark.broadcast._
import scala.collection.mutable.ListBuffer
import appraisal.spark.util.Util
import org.apache.spark.sql.functions._
import scala.collection.mutable.HashMap
import appraisal.spark.interfaces.ImputationAlgorithm
import org.apache.spark.broadcast._
import appraisal.spark.statistic.Statistic
import scala.collection.parallel.immutable._

class KnnBoosted extends ImputationAlgorithm {

  def run(idf: DataFrame, cdf:DataFrame = null,params: HashMap[String, Any] = null): Entities.ImputationResult = {

    val attribute: String = params("imputationFeature").asInstanceOf[String]
    val calcCol: Array[String] = params("calcFeatures").asInstanceOf[Array[String]]
    val T: Int = params("T").asInstanceOf[Int]
    val learningRate = params("learningRate").asInstanceOf[Double]

    val k_start: Int = params("k").asInstanceOf[Int]
    var kLimit = params("kLimit").asInstanceOf[Int]
    val varianceDfCompl = params("varianceComplete").asInstanceOf[Double]

    val fidf = idf

    val context = fidf.sparkSession.sparkContext

    val columns = fidf.columns

    // Busco o dataset completo sem os nulos
    val befCalcDf = fidf.filter(t => t.get(columns.indexOf(attribute)) != null)

    val calcDf = befCalcDf.collect().toList.par

    // Busco o dataset com os nulos para serem imputados
    val befImpDf = fidf.filter(t => t.get(columns.indexOf(attribute)) == null)

    //preciso que seja var para atualizar os pesos a cada iteração de T
    var impDf = befImpDf.collect().toList.par

    //Dados da feature a ser imputada completa (para cálculo da variância)
    val arrBefCalcDf = befCalcDf.select(col(attribute)).collect()
      .map(_.toSeq.toArray)
      .flatten
      .map(x => Util.extractDouble(x))

    if (kLimit > calcDf.size) kLimit = calcDf.size

    if (kLimit <= k_start)
      return null

    val ks = context.parallelize((k_start to kLimit))

    var boostStats = new HashMap[Int, List[Double]]()

    var rdf: ParSeq[(Long, Double, Double, Int, Double, Double, Double)] = ParSeq()
    var rdfAcc: ParSeq[(Long, Double, Double, Int, Double, Double, Double)] = ParSeq()

    for (i <- 1 to T.toInt) {

      var listStats: List[Double] = List[Double]()

      rdf = runKnn(impDf, columns, calcCol, calcDf, ks, i)

      rdfAcc = rdfAcc.++(rdf)

      // Média do k no Knn
      val count = rdf.size.doubleValue()
      val kavg = rdf.map(_._4).reduce((x, y) => x + y).doubleValue() / count

      // Calculo de normalização do erro para realizar a atualização dos pesos
      val nrdf = Util.calcExtError(rdf)

      //Tive que fazer isso para forçar precisão de duas casas decimais
      val arrImputated = rdf.map(x => x._3 - (x._3 % 0.01)).toArray
      val arrComplete = arrBefCalcDf ++ arrImputated

      //cálculo da estimativa de erro por T
      val estimator_error = nrdf.map(_._7).sum
      val beta = Util.calculateBeta(estimator_error)
      val estimator_weight = Util.calculateEstimatorWeight(beta, learningRate)
      val varianceImputated = Util.variance(arrComplete).get

      listStats = listStats :+ estimator_error
      listStats = listStats :+ varianceImputated
      listStats = listStats :+ kavg
      
      val weightUpdated = nrdf.map(x =>
        {
          val lineId = x._1
          val simpleError = x._6
          var weight = x._8

          weight = weight * math.pow(beta, (1 - simpleError) * learningRate)

          (
            lineId,
            weight)
        }).toArray

      val weightUpdatedProb = weightUpdated.map(x =>
        {
          val lineId = x._1
          val sumWeight = weightUpdated.map(_._2).sum

          val weightProb = x._2 / sumWeight
          (lineId, weightProb)
        }).sortBy(_._2)

      val weightedLines = Util.weightedSample(weightUpdatedProb, weightUpdated.length)

      impDf = Util.resampleByWheitedSample(weightedLines, impDf, columns, weightUpdated)

      boostStats = boostStats ++: HashMap(i -> listStats)
    }

    Statistic.statisticInfo(Entities.ImputationResult(
      context.parallelize(rdfAcc.map(r => Entities.Result(r._1, r._2, r._3, r._5)).toList),
      0, //kavg.intValue(),
      0,
      0,
      0,
      varianceDfCompl,
      boostStats.toSeq.sortBy(_._1),
      params.toString()))
  }

  def runKnn(impDf: ParSeq[Row], columns: Array[String], calcCol: Array[String], calcDf: scala.collection.parallel.immutable.ParSeq[org.apache.spark.sql.Row], ks: org.apache.spark.rdd.RDD[Int], t: Int) = {

    val rdf = impDf.map(row => {

      val lineId = row.getLong(columns.indexOf("lineId"))
      val originalValue = row.getDouble(columns.indexOf("originalValue"))

      //preciso fazer isso para buscar sempre o peso atualizado
      val weight = row.getDouble(if (t == 1) columns.indexOf("weight") else columns.indexOf("weight") + (t - 1))

      val lIdIndex = columns.indexOf("lineId")
      val oValIndex = columns.indexOf("originalValue")
      val cColPos = calcCol.map(columns.indexOf(_))

      val dist = calcDf.map(rowc => (rowc.getLong(lIdIndex), rowc.getDouble(oValIndex), Util.euclidianDist(row, rowc, cColPos))).toArray
        .sortBy(_._3)

      //val dist = impDf.value.sparkSession.sparkContext.broadcast(Util.euclidianDist(row, calcDf, calcCol).sortBy(_._3).collect())
      //val dist = Util.euclidianDist(row, calcDf, calcCol).sortBy(_._3).collect()

      //Busco o melhor k para cada imputação
      val impByK = ks.map(k => {

        var impValue = dist.take(k).map(_._2).reduce((x, y) => x + y) / k
        impValue = impValue - (impValue % 0.01)

        //Custo por predição

        val error = scala.math.sqrt(scala.math.pow(impValue - originalValue, 2)).doubleValue()
        val percentualError = ((error / originalValue) * 100).doubleValue()

        (k, impValue, percentualError, error)

      })

      val impByKSelected = impByK.sortBy(_._3, true).first()
      val bestK = impByKSelected._1
      val imputationValue = impByKSelected._2
      val error = impByKSelected._4
      val simpleError = imputationValue - originalValue

      (
        lineId,
        originalValue,
        imputationValue,
        bestK,
        error,
        simpleError,
        weight)
    })

    rdf
  }

  def name(): String = { "KnnBoosted" }
}