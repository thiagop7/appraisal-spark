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
import org.apache.spark.ml.linalg.{ Vector, Vectors }
import org.apache.spark.ml.regression.{ RandomForestRegressionModel, RandomForestRegressor }

class RandomForest extends ImputationAlgorithm {

  val toDouble = udf[Double, String](_.toDouble)

  def run(idf: DataFrame, cdf: DataFrame, params: HashMap[String, Any] = null): Entities.ImputationResult = {

    val attribute: String = params("imputationFeature").asInstanceOf[String]
    val calcCol: Array[String] = params("calcFeatures").asInstanceOf[Array[String]]
    val learningRate = params("learningRate").asInstanceOf[Double]

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

    val nvDf = cdf.select(attribute, "lineId").withColumn("originalValue", col(attribute))
    nvDf.show()
    fidf.show()

    val originalValues = nvDf.join(fidf, "lineId").select("originalValue", "lineId")
    originalValues.show()

    val df3 = Util.transformToMLlib(calcDf, attribute, sqlContext, context)
    var df4 = Util.transformToMLlib(impDf, attribute, sqlContext, context)
              .drop(attribute, "label")
              .join(originalValues, "lineId")
              .select("features", "lineId","originalValue")
              .withColumn(attribute, col("originalValue"))
              .withColumn("label", col("originalValue"))
              .drop("originalValue")
              
    df3.printSchema()
    df4.printSchema()

    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setHandleInvalid("keep")
      .fit(df3)

    // Train a RandomForest model.
    val rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")

    // Chain indexer and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(featureIndexer, rf))

    // Train model. This also runs the indexer.
    val model = pipeline.fit(df3)

    // Make predictions.
    val predictions = model.transform(df4)

    predictions.printSchema()

    // Select example rows to display.
    predictions.select("prediction", attribute, "features", "lineId").show(5)

    // Select (prediction, true label) and compute test error.
    val evaluator = new RegressionEvaluator()
      .setLabelCol(attribute)
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse")

    val rfModel = model.stages(1).asInstanceOf[RandomForestRegressionModel]
    println(s"Learned regression forest model:\n ${rfModel.toDebugString}")


    val stats =  predictions.select("prediction", attribute, "lineId").collect().toSeq
       
    val dfImputed = predictions.select("prediction").toDF().collect()
    
    val dfImputedDouble = dfImputed.map(x =>
      {
        val array = x.toSeq.toArray
        val arrDouble = array.map(_.toString().toDouble)
        Vectors.dense(arrDouble).toArray
      }).flatten
    
    val arrComplete = arrBefCalcDf ++ dfImputedDouble
    
    val varianceImputated = Util.variance(arrComplete).get
    
    Statistic.statisticInfo(Entities.ImputationResult(context.sparkContext.parallelize(stats.map(r => Entities.Result(r.getAs("prediction"), r.getAs(attribute), r.getAs("lineId"))).toList), 
                                                                              0, 0, 0, 0, varianceDfCompl, varianceImputated, null, params.toString()))
  }

  def name(): String = { "RandomForest" }
}