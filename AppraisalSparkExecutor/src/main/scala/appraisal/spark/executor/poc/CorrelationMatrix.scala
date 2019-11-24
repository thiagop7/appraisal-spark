package appraisal.spark.executor.poc

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import appraisal.spark.executor.util.Util
import appraisal.spark.eraser.Eraser
import appraisal.spark.entities._
import appraisal.spark.algorithm._
import appraisal.spark.statistic._
import org.apache.spark.sql.functions._
import scala.collection.mutable.HashMap
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics

object CorrelationMatrix {
  
  def main(args: Array[String]) {
    
    try{
      
      Logger.getLogger("org").setLevel(Level.ERROR)
      
      val spark = SparkSession
        .builder
        .appName("LinearRegressionDF")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
        .getOrCreate()
        
      val formatter = java.text.NumberFormat.getInstance
      
      var dfbc = Util.loadBreastCancer(spark)
      val featuresbc = Util.breastcancer_features
      var removeCol = dfbc.columns.diff(featuresbc)
      dfbc = dfbc.drop(removeCol: _*)
      val bcschema = dfbc.schema
      val arrdbc = dfbc.rdd.map(r => r.toSeq.map(_.toString.toDouble).toArray).collect()
      val rddvectbc = spark.sparkContext.parallelize(arrdbc.map(r => Vectors.dense(r)))
      val matcorrbc = Statistics.corr(rddvectbc, method="pearson")
      val rddcorrbc = toRDD(matcorrbc, spark.sparkContext).map(_.toArray.map(l => formatter.format(l)))
      val rowrddbc = rddcorrbc.map(r => Row(r: _*))
      var dfcorrbc = spark.createDataFrame(rowrddbc, bcschema)
      dfcorrbc.createOrReplaceTempView("dfcorr")
      dfcorrbc = dfcorrbc.sqlContext.sql("select * from dfcorr")
      
      Logger.getLogger("appraisal").error(dfcorrbc.show(dfcorrbc.count().intValue(), false))
      
      var count = 0
      var irdd = dfcorrbc.rdd.zipWithIndex().map(l => (l._2, l._1))
      
      for(i <- 0 to (dfcorrbc.count().intValue() - 1)){
      
        val row = irdd.lookup(i).head
        
        for(r <- 0 to (row.length - 1)){
          
          val v = row.getAs[String](r).replace(",", ".").toDouble
          if(v > 0.5)
            count = count + 1
        
        }
            
      }
        
      Logger.getLogger("appraisal").error("Number of correlations greater than 50%: " + count)
      
      Logger.getLogger("appraisal").error("")
      
      var dfaids = Util.loadAidsOccurenceAndDeath(spark)
      val featuresaids = Util.aidsocurrence_features
      removeCol = dfaids.columns.diff(featuresaids)
      dfaids = dfaids.drop(removeCol: _*)
      val aidsschema = dfaids.schema
      val arrdaids = dfaids.rdd.map(r => r.toSeq.map(_.toString.toDouble).toArray).collect()
      val rddvectaids = spark.sparkContext.parallelize(arrdaids.map(r => Vectors.dense(r)))
      val matcorraids = Statistics.corr(rddvectaids, method="pearson")
      val rddcorraids = toRDD(matcorraids, spark.sparkContext).map(_.toArray.map(l => formatter.format(l)))
      val rowrddaids = rddcorraids.map(r => Row(r: _*))
      var dfcorraids = spark.createDataFrame(rowrddaids, aidsschema)
      dfcorraids.createOrReplaceTempView("dfcorraids")
      dfcorraids = dfcorraids.sqlContext.sql("select * from dfcorraids")
      
      Logger.getLogger("appraisal").error(dfcorraids.show(dfcorraids.count().intValue(), false))
      
      count = 0
      irdd = dfcorraids.rdd.zipWithIndex().map(l => (l._2, l._1))
      
      for(i <- 0 to (dfcorrbc.count().intValue() - 1)){
      
        val row = irdd.lookup(i).head
        
        for(r <- 0 to (row.length - 1)){
          
          val v = row.getAs[String](r).replace(",", ".").toDouble
          if(v > 0.5)
            count = count + 1
        
        }
            
      }
        
      Logger.getLogger("appraisal").error("Number of correlations greater than 50%: " + count)
      
    }catch{
      
      case ex : Throwable => Logger.getLogger("appraisal").error(ex)
      
    }
    
  }
  
  def toRDD(m: Matrix, sc:SparkContext): org.apache.spark.rdd.RDD[Vector] = {
    val columns = m.toArray.grouped(m.numRows)
    val rows = columns.toSeq.transpose // Skip this if you want a column-major RDD.
    val vectors = rows.map(row => new DenseVector(row.toArray))
    sc.parallelize(vectors)
  }
  
}