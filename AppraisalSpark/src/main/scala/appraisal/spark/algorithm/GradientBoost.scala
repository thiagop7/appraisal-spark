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
import org.apache.spark.ml.regression.{ GBTRegressionModel, GBTRegressor }

class GradientBoost extends ImputationAlgorithm {

  val toDouble = udf[Double, String](_.toDouble)

  // Let's create a UDF to take array of embeddings and output Vectors
  val convertToVectorUDF = udf((matrix: Seq[Double]) => {
    Vectors.dense(matrix.toArray)
  })

  def transformToMLlib(df: DataFrame, attribute: String, sqlContext: SQLContext, context: SparkSession): DataFrame = {

    val indexedFeatures = df.select(attribute,"lineId").withColumn("label", col(attribute)).withColumn("id", monotonically_increasing_id())
    val df2 = sqlContext.createDataFrame(indexedFeatures.rdd, indexedFeatures.schema)

    import context.implicits._

    val dfFeatures = df.rdd.map(x =>
      {
        val seq = x.toSeq
        seq.map(x =>
          {
            Util.extractDouble(x)
          })

      }).toDF("features")
      .withColumn("features", convertToVectorUDF($"features"))
      .withColumn("id", monotonically_increasing_id())

    dfFeatures.join(df2, "id").drop("id")
  }

  
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

    val df3 = transformToMLlib(calcDf, attribute, sqlContext, context)
    var df4 = transformToMLlib(impDf, attribute, sqlContext, context)
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
    val rf = new GBTRegressor()
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
    predictions.select("prediction", attribute, "features").show(5)

    // Select (prediction, true label) and compute test error.
    val evaluator = new RegressionEvaluator()
      .setLabelCol(attribute)
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse")

    val rfModel = model.stages(1).asInstanceOf[GBTRegressionModel]
    println(s"Learned regression forest model:\n ${rfModel.toDebugString}")

    return null
  }
  
  def name(): String = { "GradientBoost" }

}