package appraisal.spark.algorithm

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import appraisal.spark.entities._
import appraisal.spark.util.Util
import appraisal.spark.interfaces.ClusteringAlgorithm
import scala.collection.mutable.HashMap
import org.apache.spark.broadcast._

class KMeans extends ClusteringAlgorithm {
  
  def run(idf: DataFrame, cdf:DataFrame = null, params: HashMap[String, Any] = null): Entities.ClusteringResult = {
    
    val k: Int =  params("k").asInstanceOf[Int]
    val maxIter: Int = params("maxIter").asInstanceOf[Int]
    val calcCol: Array[String] = params("calcFeatures").asInstanceOf[Array[String]]
    
    val columns = idf.columns
    
    val lineIdPos = columns.indexOf("lineId")
    
    val vectorsRdd = idf.rdd.map(row => {
      
      val lineId = row.getLong(lineIdPos)
      
      var values = new Array[Double](calcCol.length)
      var index = -1
      
      for(i <- 0 to (calcCol.length - 1))
        values(i) = row.getDouble(columns.indexOf(calcCol(i)))
        
      (lineId, Vectors.dense(values))
      
    }).cache()
    
    val vectors = vectorsRdd.map(_._2)
    
    val kMeansModel = org.apache.spark.mllib.clustering.KMeans.train(vectors, k, maxIter)
    
    val wssse = kMeansModel.computeCost(vectors)
    
    val res = vectorsRdd.map(x => Entities.CResult(kMeansModel.predict(x._2), x._1))
    
    return Entities.ClusteringResult(res, k, wssse)
    
  }
  
  def name(): String = {"KMeans"}
  
}