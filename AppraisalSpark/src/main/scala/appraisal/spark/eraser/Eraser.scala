package appraisal.spark.eraser

import org.apache.spark.sql._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions._
import org.apache.spark.broadcast._

class Eraser extends Serializable {
  
  def run(odf: DataFrame, attribute: String, percent: Double): DataFrame = {
   
   var nodf = odf
   
   val qtd = ((nodf.count() * percent) / 100).toInt
   
   nodf.createOrReplaceTempView("originaldb")
   
   val niddf = nodf.sqlContext.sql("select lineId from originaldb order by rand() limit " + qtd)
   niddf.createOrReplaceTempView("niddf")
   
   val ncoldf = niddf.sqlContext.sql("select o.lineId, case when o.lineId == n.lineId then null else o." 
   + attribute + " end as ncolumn from originaldb o left join niddf n on o.lineId = n.lineId")
   ncoldf.createOrReplaceTempView("ncoldf")
   
   var rdf = niddf.sqlContext.sql("select o.*, n.ncolumn from originaldb o, ncoldf n where o.lineId == n.lineId")
   rdf = rdf.withColumn(attribute, rdf("ncolumn")).drop("ncolumn")
   
   return rdf
    
  }
  
}