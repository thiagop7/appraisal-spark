package appraisal.spark.algorithm

import appraisal.spark.interfaces.SelectionAlgorithm
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import appraisal.spark.entities._
import appraisal.spark.util.Util
import scala.collection.mutable.HashMap
import org.apache.spark.broadcast._

class Pca extends SelectionAlgorithm {
  
  def run(odf: DataFrame, params: HashMap[String, Any] = null): Entities.SelectionResult = {
    
    val attribute: String = params("imputationFeature").asInstanceOf[String] 
    val percentReduction: Double = params("percentReduction").asInstanceOf[Double] 
    
    val context = odf.sparkSession.sparkContext
    
    val dropss = Array[String]("lineId", "originalValue")
    val _odf = odf.drop(dropss: _*)
    
    val columns = _odf.columns
    
    val qtdAttributes = columns.length
    
    val pcq = ((1 - (percentReduction / 100)) * qtdAttributes).intValue()
    
    val vectorsRdd = _odf.rdd.map(row => {
      
      val length = columns.length
      val fLength = length - 1
      var values = new Array[Double](length)
      
      for(i <- 0 to fLength)
        values(i) = row.getDouble(i)
        
      (Vectors.dense(values))
      
    }).cache()
    
    val matrix: RowMatrix = new RowMatrix(vectorsRdd)
    
    val cvla = matrix.computeCovariance()
    
    val attributeIndex = columns.indexOf(attribute)
    
    val sres = (0 to (cvla.numCols - 1)).toArray.map(l => (cvla.apply(attributeIndex, l).abs, l))
                .filter(_._2 != attributeIndex).sortBy(_._1).reverse
                .take(pcq).map(l => (columns(l._2), columns.indexOf(columns(l._2)), l._1))
                .zipWithIndex.map(l => (l._2, l._1))
    
    val rddres = context.parallelize(sres).map(l => Entities.SResult(l._1, l._2._1, l._2._2, l._2._3))
    Entities.SelectionResult(rddres)
    
  }
  
  def name(): String = {"Pca"}
  
}