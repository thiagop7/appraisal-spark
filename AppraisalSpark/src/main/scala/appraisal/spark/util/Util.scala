package appraisal.spark.util

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.rdd._
import org.apache.spark.broadcast._
import appraisal.spark.entities._
import org.apache.spark.ml.linalg.{ Vector, Vectors }
import scala.math.log
import scala.collection.parallel.immutable._


object Util {

  val toDouble = udf[Option[Double], String](x => if (x != null) Some(x.toDouble) else null)

  val toLong = udf[Long, String](_.toLong)

  def isNumeric(str: String): Boolean = str.matches("[-+]?\\d+(\\.\\d+)?")

  def isNumericOrNull(obj: Any): Boolean = {

    if (obj == null) return true

    obj.toString().matches("[-+]?\\d+(\\.\\d+)?")

  }

  def euclidianDist(row: Row, rowc: Row, cColPos: Array[Int]): Double = {

    var dist = 0d

    cColPos.foreach(attIndex => {

      dist += scala.math.pow(row.getDouble(attIndex) - rowc.getDouble(attIndex), 2)

    })

    return scala.math.sqrt(dist)

  }

  def filterNullAndNonNumeric(df: DataFrame, columns: Array[String] = null): DataFrame = {

    var _columns = df.columns
    if (columns != null)
      _columns = columns

    var rdf = df

    _columns.foreach(column => {

      val columnIndex = rdf.columns.indexOf(column)
      rdf = filterNullAndNonNumericByAtt(rdf, columnIndex)

    })

    rdf

  }

  def filterNullAndNonNumericByAtt(df: DataFrame, attIndex: Int): DataFrame = {

    df.filter(r => r.get(attIndex) != null && Util.isNumeric(r.get(attIndex).toString()))

  }

  def hasNullatColumn(df: DataFrame, feature: String): Boolean = {

    df.createOrReplaceTempView("imputationdb")
    val nullCount = df.sqlContext.sql("select count(*) from imputationdb where " + feature + " is null").head().getAs[Long](0)
    nullCount > 0

  }

  def getCurrentTime() = {

    val dateFormatter = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
    var submittedDateConvert = new java.util.Date()
    dateFormatter.format(submittedDateConvert)

  }

  def getCurrentTime(date: java.util.Date) = {

    val dateFormatter = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
    dateFormatter.format(date)

  }

  def combineResult(results: Seq[Entities.ImputationResult]): Entities.ImputationResult = {

    val r = results

    val count = results.size

    val k = r.map(_.k.asInstanceOf[Int]).reduce((x, y) => x + y) / count
    val totalError = r.map(_.totalError.asInstanceOf[Double]).reduce((x, y) => x + y) / count
    val avgError = r.map(_.avgError.asInstanceOf[Double]).reduce((x, y) => x + y) / count
    val avgPercentualError = r.map(_.avgPercentError.asInstanceOf[Double]).reduce((x, y) => x + y) / count
    val varianceImputedError = r.map(_.varianceImputedError.asInstanceOf[Double]).reduce((x, y) => x + y) / count
    val varianceCompleteError = r.map(_.varianceCompleteError.asInstanceOf[Double]).reduce((x, y) => x + y) / count

    Entities.ImputationResult(null, k.intValue(), avgError, totalError, avgPercentualError, varianceCompleteError, varianceImputedError, null, null)

  }

  def mean(xs: Array[Double]): Option[Double] = {

    if (xs.isEmpty) None
    else Some(xs.sum / xs.length)

  }

  def variance(xs: Array[Double]): Option[Double] = {

    mean(xs).flatMap(m => mean(xs.map(x => Math.pow(x - m, 2))))

  }
  def extractDouble: (Any) => Double = {
    case i: Int    => i.toDouble
    case f: Float  => f.toDouble
    case d: Double => d.toDouble
    case l: Long   => l.toDouble
    case s: String => s.toDouble    
  }
  
  // Métodos utilizados para cálculo dos pesos e realização do weighted bootstrap sample

  // a very simple weighted sampling function
  def weightedSample(dist: Array[(Long, Double)], numSamples: Int): Array[Long] = {

    val probs = dist.zipWithIndex.map { case ((elem, prob), idx) => (elem, prob, idx + 1) }.sortBy(-_._2)
    val cumulativeDist = probs.map(_._2).scanLeft(0.0)(_ + _).drop(1)
    (1 to numSamples).toArray.map(x => scala.util.Random.nextDouble).map {
      case (p) =>

        def findElem(p: Double, cumulativeDist: Array[Double]): Int = {

          for (i <- (0 until cumulativeDist.size - 1))
            if (p <= cumulativeDist(i)) return i
          return cumulativeDist.size - 1

        }

        probs(findElem(p, cumulativeDist))._1
    }
  }

  // Let's create a UDF to take array of embeddings and output Vectors
  val convertToVectorUDF = udf((matrix: Seq[Double]) => {
    Vectors.dense(matrix.toArray)
  })

  def transformToMLlib(df: DataFrame, attribute: String, sqlContext: SQLContext, context: SparkSession): DataFrame = {

    val indexedFeatures = df.select(attribute, "lineId").withColumn("label", col(attribute)).withColumn("id", monotonically_increasing_id())
    val df2 = sqlContext.createDataFrame(indexedFeatures.rdd, indexedFeatures.schema)

    import context.implicits._

    val dfFeatures = df.rdd.map(x =>
      {
        val seq = x.toSeq
        seq.map(x =>
          {
            extractDouble(x)
          })

      }).toDF("features")
      .withColumn("features", convertToVectorUDF($"features"))
      .withColumn("id", monotonically_increasing_id())

    dfFeatures.join(df2, "id").drop("id")
  }

  def calculateBeta(estimator_error: Double): Double = {

    //Low β means high confidence in theprediction.
    return estimator_error / (1 - estimator_error)
  }

  def calculateEstimatorWeight(beta: Double, learningRate: Double): Double = {
    val logBeta = log(1 / beta)

    return learningRate * logBeta
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

}