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
import scala.math.log
import scala.collection.parallel.immutable._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.regression.DecisionTreeRegressor

class AdaboostR2_old extends ImputationAlgorithm {

  def run(idf: DataFrame, cdf: DataFrame = null, params: HashMap[String, Any] = null): Entities.ImputationResult = {

    val attribute: String = params("imputationFeature").asInstanceOf[String]
    val calcCol: Array[String] = params("calcFeatures").asInstanceOf[Array[String]]
    val T: Int = params("T").asInstanceOf[Int]
    val learningRate = params("learningRate").asInstanceOf[Double]

    val k_start: Int = params("k").asInstanceOf[Int]
    var kLimit = params("kLimit").asInstanceOf[Int]
    val varianceDfCompl = params("varianceComplete").asInstanceOf[Double]

    val fidf = idf

    //val context = fidf.sparkSession.sparkContext
    val context = fidf.sparkSession
    val sqlContext = idf.sqlContext

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

    var boostStats = new HashMap[Int, List[Double]]()

    var rdf: ParSeq[(Long, Double, Double, Int, Double, Double, Double)] = ParSeq()
    var rdfAcc: ParSeq[(Long, Double, Double, Int, Double, Double, Double)] = ParSeq()

    for (i <- 1 to T.toInt) {

      var listStats: List[Double] = List[Double]()

      rdf = runDecisionTree(impDf, columns, calcCol, calcDf, context, i)

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
      context.sparkContext.parallelize(rdfAcc.map(r => Entities.Result(r._1, r._2, r._3, r._5)).toList),
      0, //kavg.intValue(),
      0,
      0,
      0,
      varianceDfCompl,
      0,
      boostStats.toSeq.sortBy(_._1),
      params.toString()))
  }

  def runDecisionTree(impDf: ParSeq[Row], columns: Array[String], calcCol: Array[String], calcDf: ParSeq[Row], context: SparkSession, t: Int) = {

    var rdf: ParSeq[(Long, Double, Double, Int, Double, Double, Double)] = ParSeq()

    val arr = impDf.toArray

    import context.implicits._

    val dfFeatures = arr.map(x =>
      {
        val seq = x.toSeq.toArray
        seq.map(x =>
          {
            Util.extractDouble(x)
          })

      })

    val df = context.sparkContext.parallelize(dfFeatures).toDF()

    // Automatically identify categorical features, and index them.
    // Here, we treat features with > 4 distinct values as continuous.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setHandleInvalid("keep")
      .fit(df)

      
      
    rdf
  }

  def resampleByWheitedSample(weightedLines: Array[Long], origImpDf: ParSeq[Row], columns: Array[String], linesWeight: Array[(Long, Double)]): ParSeq[Row] = {

    val lineIdIndex = columns.indexOf("lineId")
    val originalValue = columns.indexOf("originalValue")
    val indexWeight = columns.indexOf("weight")

    //Removo a coluna de peso para atualizar com o novo peso
    val allColumns = columns.dropRight(1)

    val lines = origImpDf.filter(row => weightedLines.toList.contains(row.getLong(lineIdIndex)))

    val newLines = lines.map(x =>
      {

        val newWeight = linesWeight.filter(p => p._1 == x.getLong(lineIdIndex)).last._2

        (Row.fromSeq(Seq(newWeight).++:(x.toSeq)))

      })

    return newLines

  }

  def calcExtError(rdf: ParSeq[(Long, Double, Double, Int, Double, Double, Double)]) = { // Calculo de normalização do erro para realizar a atualização dos pesos

    val maxErr = rdf.map(_._5).max

    val nrdf = rdf.map(r =>
      {

        var erroNorm = r._5
        var estimatorError = 0.0
        if (maxErr != 0) {
          erroNorm = erroNorm / maxErr
          estimatorError = r._7 * erroNorm
        }

        (
          r._1,
          r._2,
          r._3,
          r._4,
          r._5,
          erroNorm,
          estimatorError,
          r._7)
      })
    nrdf
  }

  def calculateBeta(estimator_error: Double): Double = {

    //Low β means high confidence in theprediction.
    return estimator_error / (1 - estimator_error)
  }

  def calculateEstimatorWeight(beta: Double, learningRate: Double): Double = {
    val logBeta = log(1 / beta)

    return learningRate * logBeta
  }

  def name(): String = { "AdaboostR2" }

}