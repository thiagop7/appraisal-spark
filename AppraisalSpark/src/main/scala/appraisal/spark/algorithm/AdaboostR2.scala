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
import org.apache.spark.ml.regression.BoostingRegressor
import org.apache.spark.sql.functions.{ rand, when }
import org.apache.spark.ml.tuning.{ CrossValidator, ParamGridBuilder }
import org.apache.spark.ml.regression.BoostingRegressionModel

class AdaboostR2 extends ImputationAlgorithm {

  def run(idf: DataFrame, cdf: DataFrame = null, params: HashMap[String, Any] = null): Entities.ImputationResult = {

    val attribute: String = params("imputationFeature").asInstanceOf[String]
    val calcCol: Array[String] = params("calcFeatures").asInstanceOf[Array[String]]

    val varianceDfCompl = params("varianceComplete").asInstanceOf[Double]

    val fidf = idf

    val context = fidf.sparkSession
    val sqlContext = idf.sqlContext

    val columns = fidf.columns

    // Busco o dataset completo sem os nulos
    val befCalcDf = fidf.filter(t => t.get(columns.indexOf(attribute)) != null)

    val calcDf = befCalcDf.toDF()

    // Busco o dataset com os nulos para serem imputados
    val befImpDf = fidf.filter(t => t.get(columns.indexOf(attribute)) == null).drop(attribute).withColumn(attribute, lit(0.toDouble))

    //preciso que seja var para atualizar os pesos a cada iteração de T
    var impDf = befImpDf.toDF()

    //Dados da feature a ser imputada completa (para cálculo da variância)
    val arrBefCalcDf = befCalcDf.select(col(attribute)).collect()
      .map(_.toSeq.toArray)
      .flatten
      .map(x => Util.extractDouble(x))

    val nvDf = cdf.select(attribute, "lineId") //.withColumn("originalValue", col(attribute))

    val originalValues = nvDf.join(fidf, "lineId").select("originalValue", "lineId")

    val df3 = Util.transformToMLlib(calcDf, attribute, sqlContext, context)
      .withColumn("val", lit(false))
      .drop(attribute)
      .drop("lineId")

    var df4 = Util.transformToMLlib(impDf, attribute, sqlContext, context)
      .drop(attribute, "label")
      .join(originalValues, "lineId")
      .select("features", "lineId", "originalValue")
      .withColumn("label", col("originalValue"))
      .withColumn("val", lit(true))
      .drop("originalValue")
      .drop("lineId")

    val full = df4.union(df3)
    full.cache().first()

    val dr = new DecisionTreeRegressor()
    val br = new BoostingRegressor()
      .setBaseLearner(dr)

    val re = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val brParamGrid = new ParamGridBuilder()
      .addGrid(br.loss, Array("squared"))
      .addGrid(br.validationIndicatorCol, Array("val"))
      .addGrid(br.numBaseLearners, Array(20))
      .addGrid(br.tol, Array(1E-9))
      .addGrid(br.numRound, Array(3))
      .addGrid(dr.maxDepth, Array(10))
      .build()

    val brCV = new CrossValidator()
      .setEstimator(br)
      .setEvaluator(re)
      .setEstimatorParamMaps(brParamGrid)
      .setNumFolds(3)
      .setParallelism(4)

    full.printSchema()

    val brCVModel = brCV.fit(full)

    // Make predictions.
    val predictions = brCVModel.transform(full)

    predictions.printSchema()

    // Select example rows to display.
    predictions.select("prediction", "label", "lineId").show(5)

    val rmse = re.evaluate(predictions)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse")

    println(brCVModel.avgMetrics.mkString(","))
    println(brCVModel.bestModel.asInstanceOf[BoostingRegressionModel].getLoss)
    println(brCVModel.bestModel.asInstanceOf[BoostingRegressionModel].models.length)
    println(brCVModel.bestModel.asInstanceOf[BoostingRegressionModel].weights.mkString(","))
    println(brCVModel.avgMetrics.min)

    return null
  }
  def name(): String = { "AdaboostR2" }
}