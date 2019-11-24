package appraisal.spark.engine

import appraisal.spark.interfaces._
import org.apache.spark.sql._
import appraisal.spark.strategies._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.types.{LongType, IntegerType}
import appraisal.spark.util.Util
import appraisal.spark.statistic.Statistic
import org.apache.log4j.Logger
import org.apache.spark.broadcast._
import org.apache.spark.sql.functions._
import appraisal.spark.entities._
import appraisal.spark.strategies.AppraisalStrategy

class ImputationPlan(idf: DataFrame, odf: DataFrame, missingRate: Double, imputationFeature: String, features: Array[String], parallel: Boolean = true) extends Serializable {
  
  var strategies = List.empty[AppraisalStrategy]
  var planName = "";
  var calcCol: Array[String] = null
  var ensembleStrategy: EnsembleStrategy = null
  
  def getIdf :DataFrame = {
    idf
  }
  
  def getOdf :DataFrame = {
    odf
  }
  
  def getMissingRate :Double = {
    missingRate
  }
  
  def getImputationFeature :String = {
    imputationFeature
  }
  
  def getFeatures :Array[String] = {
    features
  }
  
  def getParallel :Boolean = {
    parallel
  }
  
  def addStrategy(strategy :AppraisalStrategy) = {
    
    strategies = strategies :+ strategy
    
    if(!"".equals(planName)){
      
      planName += "->"
    
    }
    
    planName += strategy.strategyName + "[" + strategy.algName() + "]"
    
  }
  
  def addEnsembleStrategy(_ensembleStrategy :EnsembleStrategy) = {
    ensembleStrategy = _ensembleStrategy
  }
  
  def updateStrategies(_strategies: List[AppraisalStrategy]) = {
    
    _strategies.foreach(strat => addStrategy(strat))
    
  }
  
  def run() :Entities.ImputationResult = {
    
    if(ensembleStrategy == null){
    
      val planStartTime = new java.util.Date()
      
      var logStack = List.empty[String]
      
      logStack = logStack :+ "-------------------------------------"
      logStack = logStack :+ "Running imputation plan: " + planName
      logStack = logStack :+ "Missing rate at " + missingRate + "% in feature " + imputationFeature
      
      var firs :Entities.ImputationResult = null;
      
      var _idf = idf
      var _odf = odf
      var _features = features
      
      val context = _idf.sparkSession.sparkContext
      
      val remCol = _odf.columns.diff(_features).filter(c => !"lineId".equals(c) && !imputationFeature.equals(c) && !"originalValue".equals(c) && !"weight".equals(c))
      _odf = _odf.drop(remCol: _*)
      _odf = appraisal.spark.util.Util.filterNullAndNonNumeric(_odf)
      
      _odf.columns.filter(!"lineId".equals(_)).foreach(att => _odf = _odf.withColumn(att, appraisal.spark.util.Util.toDouble(col(att))))
     
      calcCol = _features.filter(!_.equals(imputationFeature))
      val removeCol = _idf.columns.diff(_features).filter(c => !"lineId".equals(c) && !imputationFeature.equals(c) && !"originalValue".equals(c) && !"weight".equals(c))
      
      var vnidf = appraisal.spark.util.Util.filterNullAndNonNumeric(_idf.drop(removeCol: _*), calcCol)
      
      val impcolindex = vnidf.columns.indexOf(imputationFeature)
      vnidf = vnidf.filter(r => Util.isNumericOrNull(r.get(impcolindex)))
      
      vnidf.columns.filter(!"lineId".equals(_)).foreach(att => vnidf = vnidf.withColumn(att, appraisal.spark.util.Util.toDouble(col(att))))
      var _vnidf = vnidf
      
      try{
      
        var imputationBatch = Seq.empty[DataFrame]
        var p_imputationBatch = Seq.empty[DataFrame].par
        
        //for (strategy <- strategies){
        
        var count = 0
        val strategycount = strategies.size
        
        while(count < strategycount){
          
          val strategy = strategies(count)
          
          if (strategy.isInstanceOf[SelectionStrategy]) {
            
            val ss = strategy.asInstanceOf[SelectionStrategy]
            
            if(!ss.params.contains("imputationFeature"))
              ss.params.put("imputationFeature", imputationFeature)
            else
              ss.params.update("imputationFeature", imputationFeature)
            
            logStack = logStack :+ "Running " + strategy.strategyName + "[" + strategy.algName() + "] | params: " + ss.parameters
            
            val sr = ss.run(_odf)
            
            logStack = logStack :+ "SelectionResult: " + sr.toString()
            
            val useColumns = sr.result.map(_.attribute).collect()
            calcCol = useColumns.filter(!_.equals(imputationFeature))
            _features = (useColumns :+ "lineId" :+ "originalValue" + imputationFeature).distinct
            
            val removecolumns = _features.diff(_vnidf.columns)
            _vnidf = _vnidf.drop(removecolumns :_*)
            
          }
          
          if (strategy.isInstanceOf[ClusteringStrategy]) {
            
            val cs = strategy.asInstanceOf[ClusteringStrategy]
            
            if(!cs.params.contains("calcFeatures"))
              cs.params.put("calcFeatures", calcCol)
            else
              cs.params.update("calcFeatures", calcCol)
            
            logStack = logStack :+ "Running " + strategy.strategyName + "[" + strategy.algName() + "] | params: " + cs.parameters
            
            val cr = cs.run(_vnidf)
            
            logStack = logStack :+ "ClusteringResult: " + cr.toString()
            logStack = logStack :+ "best k: " + cr.k
            
            val schema = new StructType()
            .add(StructField("cluster", IntegerType, true))
            .add(StructField("lineidcluster", LongType, true))
            
            var cdf = _vnidf.sparkSession.createDataFrame(cr.result.map(x => Row(x.cluster, x.lineId)), schema)
            val bcdf = cdf
            val impdf = _vnidf
            
            if(parallel){
              
              var clusters = cdf.rdd.map(_.getInt(0)).distinct().collect().toSeq.par
            
              p_imputationBatch = clusters.map(cluster => {
                
                bcdf.createOrReplaceTempView("clusterdb")
                impdf.createOrReplaceTempView("imputationdb")
                
                bcdf.sqlContext.sql("select imputationdb.* from imputationdb, clusterdb "
                                  + "where imputationdb.lineId = clusterdb.lineidcluster and clusterdb.cluster = " + cluster)
                
              })
              
              logStack = logStack :+ "Batch for imputation after clustering strategy: " + p_imputationBatch.size
            
            }else{
              
              var clusters = cdf.rdd.map(_.getInt(0)).distinct().collect().toSeq
            
              imputationBatch = clusters.map(cluster => {
                
                bcdf.createOrReplaceTempView("clusterdb")
                impdf.createOrReplaceTempView("imputationdb")
                
                bcdf.sqlContext.sql("select imputationdb.* from imputationdb, clusterdb "
                                  + "where imputationdb.lineId = clusterdb.lineidcluster and clusterdb.cluster = " + cluster)
                
              })
              
              logStack = logStack :+ "Batch for imputation after clustering strategy: " + imputationBatch.size
              
            }
            
          }
          
          if (strategy.isInstanceOf[ImputationStrategy]) {
             
             if(parallel){
            
               if(p_imputationBatch == null || p_imputationBatch.isEmpty) p_imputationBatch = Seq(_vnidf).par
             
             }else{
               
               if(imputationBatch == null || imputationBatch.isEmpty) imputationBatch = Seq(_vnidf)
               
             }
             
             val is = strategy.asInstanceOf[ImputationStrategy]
             
             if(!is.params.contains("calcFeatures"))
               is.params.put("calcFeatures", calcCol)
             else
               is.params.update("calcFeatures", calcCol)
               
             if(!is.params.contains("imputationFeature"))
               is.params.put("imputationFeature", imputationFeature)
             
             if(parallel){
               
               p_imputationBatch = p_imputationBatch.filter(df => df != null && df.count() > 0 && Util.hasNullatColumn(df, is.params("imputationFeature").asInstanceOf[String])) 
             
               p_imputationBatch.foreach(_.printSchema())
               
               logStack = logStack :+ "Batch for imputation before imputation strategy: " + p_imputationBatch.size
               
               if(p_imputationBatch.size > 0){
                 
                 logStack = logStack :+ "Running " + strategy.strategyName + "[" + strategy.algName() + "] | params: " + is.parameters
                 
                 val irs = p_imputationBatch.map(x => 
                   {
                     is.run(x)
                   })
                 
                 //firs = is.combineResult(irs)
                 firs = irs.filter(_ != null).toArray.sortBy(_.avgPercentError).head
                 
                 logStack = logStack :+ "ImputationResult: " + firs.toString()
                 logStack = logStack :+ "best k: " + firs.k
                 logStack = logStack :+ "totalError: " + firs.totalError
                 logStack = logStack :+ "avgError: " + firs.avgError
                 logStack = logStack :+ "avgPercentError: " + firs.avgPercentError
                 logStack = logStack :+ "BoostedStats: " + firs.boostedparams.map(x => "T: "+ x._1 + " "+ x._2)
                 logStack = logStack :+ "varianceCompleteError: " + firs.varianceCompleteError
                 
                 
               }else{
                 
                 logStack = logStack :+ "ImputationResult: EMPTY BATCH - There is no tuples for imputation, skiping plan."
                 firs = null
                 
               }
               
             }else{
               
               imputationBatch = imputationBatch.filter(df => df != null && df.count() > 0 && Util.hasNullatColumn(df, is.params("imputationFeature").asInstanceOf[String])) 
             
               logStack = logStack :+ "Batch for imputation before imputation strategy: " + imputationBatch.size
               
               if(imputationBatch.size > 0){
               
                 logStack = logStack :+ "Running " + strategy.strategyName + "[" + strategy.algName() + "] | params: " + is.parameters
                 
                 val irs = imputationBatch.map(x => 
                   {
                     is.run(x)
                   })
                 
                 //firs = is.combineResult(irs)
                 firs = irs.filter(_ != null).toArray.sortBy(_.avgPercentError).head
                 
                 logStack = logStack :+ "ImputationResult: " + firs.toString()
                 logStack = logStack :+ "best k: " + firs.k
                 logStack = logStack :+ "totalError: " + firs.totalError
                 logStack = logStack :+ "avgError: " + firs.avgError
                 logStack = logStack :+ "avgPercentError: " + firs.avgPercentError
                 //logStack = logStack :+ "varianceImputedError: " + firs.varianceImputedError
                 
                 
               }else{
                 
                 logStack = logStack :+ "ImputationResult: EMPTY BATCH - There is no tuples for imputation, skiping plan."
                 firs = null
                 
               }
               
             }
             
          }
          
          count += 1
          
        }
        
        logStack = logStack :+ "-------------------------------------"
        
      }catch{
        
        case ex : Exception => {
          
          logStack.foreach(Logger.getLogger(getClass.getName).error(_))
          Logger.getLogger(getClass.getName).error("Error executing imputation plan: " + planName, ex)
          
        }
        
      }
      
      val planStopTime = new java.util.Date()
      
      val planTimeseconds   = ((planStopTime.getTime - planStartTime.getTime) / 1000)
      
      val planTimesMinutes = planTimeseconds / 60
      
      val planTimesHours = planTimesMinutes / 60
      
      val planTime   = ((planStopTime.getTime - planStartTime.getTime) / 3600000)
      
      logStack = logStack :+ "Total plan execution time: " + planTimeseconds + " seconds, " + planTimesMinutes + " minutes, " + planTimesHours + " hours."
      
      logStack.foreach(Logger.getLogger(getClass.getName).error(_))
      
      return firs
      
    }else{
      
      ensembleStrategy.run(null)
      
    }
    
  }
  
}