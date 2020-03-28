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
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.{ CrossValidator, ParamGridBuilder }
import org.apache.spark.ml.regression.{ StackingRegressor, DecisionTreeRegressor, RandomForestRegressor, StackingRegressionModel }
import org.apache.spark.ml.linalg.{ Vector, Vectors }
import org.apache.spark.ml.regression.BoostingRegressor
import org.apache.spark.ml.regression.BaggingRegressor

class StackedGeneralization extends ImputationAlgorithm {

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

    var df4 = Util.transformToMLlib(impDf, attribute, sqlContext, context)
      .drop(attribute, "label")
      .join(originalValues, "lineId")
      .select("features", "lineId", "originalValue")
      .withColumn("label", col("originalValue"))
      .withColumn("val", lit(true))
      .drop("originalValue")

    val full = df4.union(df3).toDF()
    full.cache()

    val dr = new DecisionTreeRegressor()
      .setMaxDepth(15)
    val br = new BoostingRegressor()
      .setLoss("squared")
      .setValidationIndicatorCol("val")
      .setNumBaseLearners(200)
      .setTol(1E-9)
      .setNumRound(5)
      .setBaseLearner(dr)

    val bgr = new BaggingRegressor()      
      .setFeaturesCol("val")
      .setNumBaseLearners(200)
      .setReplacement(true)
      .setBaseLearner(dr)
      .setParallelism(2)

    val sr = new StackingRegressor()
      .setStacker(new DecisionTreeRegressor())
      .setBaseLearners(Array(br, bgr))
      .setParallelism(2)

    val re = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val srParamGrid = new ParamGridBuilder()
      .build()

    val srCV = new CrossValidator()
      .setEstimator(sr)
      .setEvaluator(re)
      .setEstimatorParamMaps(srParamGrid)
      .setNumFolds(3)
      .setParallelism(2)

    val srCVModel = srCV.fit(full)

    // Make predictions.
    val predictions = srCVModel.transform(full)

    val rmse = re.evaluate(predictions)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse")

    println(srCVModel.avgMetrics.min)

    val bm = srCVModel.bestModel.asInstanceOf[StackingRegressionModel]

    val stats = predictions.select("prediction", "label", "lineId").collect().toSeq

    val dfImputed = predictions.select("prediction").toDF().collect()

    val dfImputedDouble = dfImputed.map(x =>
      {
        val array = x.toSeq.toArray
        val arrDouble = array.map(_.toString().toDouble)
        Vectors.dense(arrDouble).toArray
      }).flatten

    val arrComplete = arrBefCalcDf ++ dfImputedDouble
    val varianceImputated = Util.variance(arrComplete).get

    val arrFeatComplete = cdf.select(col(attribute)).collect().map(_.toSeq.toArray).flatten
    val varianceBefore = Util.variance(arrFeatComplete.map(x => Util.extractDouble(x))).get

    val weights = srCVModel.bestModel.asInstanceOf[StackingRegressionModel]

    Statistic.statisticInfo(Entities.ImputationResult(
      context.sparkContext.parallelize(stats.map(r => Entities.Result(r.getAs("lineId"), r.getAs("label"), r.getAs("prediction"))).toList),
      0, 0, rmse, srCVModel.avgMetrics.min, varianceDfCompl, varianceImputated, null, params.toString()))
  }

  def name(): String = { "StackedGeneralization" }
}