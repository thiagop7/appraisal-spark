package appraisal.spark.executor.poc

import org.apache.log4j._
import appraisal.spark.engine._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import appraisal.spark.algorithm.Knn
import appraisal.spark.eraser.Eraser
import appraisal.spark.executor.util.Util
import org.apache.spark.sql.functions._
import scala.collection.mutable.HashMap
import appraisal.spark.strategies._
import appraisal.spark.algorithm._

object ImputationPlanExec extends Serializable {
  
  def main(args: Array[String]) {
    
    var parallelExecution = true
    var breastCancer = true
    var aidsOccurrence = false
    
    if(args != null && args.length > 0){
      
      if("single".equalsIgnoreCase(args(0))){
        
        parallelExecution = false
      
      }else if ("breastcancer".equalsIgnoreCase(args(0))){
        
        breastCancer = true
          
      }else if("aidsoccurrence".equalsIgnoreCase(args(0))){
        
        aidsOccurrence = true
        
      }
      
      if(args.length > 1){
        
        if ("breastcancer".equalsIgnoreCase(args(1)))
          breastCancer = true
          
        else if("aidsoccurrence".equalsIgnoreCase(args(1)))
          aidsOccurrence = true
          
      }
      
    }
      
    val wallStartTime = new java.util.Date()
    Logger.getLogger(getClass.getName).error("Appraisal Spark - Wall start time: " + appraisal.spark.util.Util.getCurrentTime(wallStartTime))
    Logger.getLogger(getClass.getName).error("Parallel execution: " + parallelExecution)
    
    try{
      
      val conf = new SparkConf()
      //.set("spark.executor.memory", "1g")
      //.set("spark.executor.cores", "8")
      .set("spark.network.timeout", "10800")
      .set("spark.sql.broadcastTimeout", "10800")
      .set("spark.executor.extraJavaOptions", "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps")
      .set("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      
      val spark = SparkSession
        .builder
        .appName("AppraisalSpark")
        .master("local[*]")
        //.master("spark://127.0.0.1:7077")
        .config(conf)
        .getOrCreate()
      
      var features: Array[String] = null
      var feature = ""
      
      var odf: DataFrame = null
      
      if(breastCancer){
        
        //odf = Util.loadData(spark, "file:///shared/appraisal/breast_cancer_wisconsin.csv").withColumn("lineId", monotonically_increasing_id)
        odf = Util.loadBreastCancer(spark).withColumn("lineId", monotonically_increasing_id)
        features = Util.breastcancer_features
      
      }else if(aidsOccurrence){
        
        odf = Util.loadData(spark, "file:///shared/appraisal/AIDS_Occurrence_and_Death_and_Queries.csv").withColumn("lineId", monotonically_increasing_id)
        features = Util.aidsocurrence_features
        
      }
      
      Logger.getLogger(getClass.getName).error("Data count: " + odf.count())
      
      //val k_nsqrt = scala.math.sqrt(odf.value.count()).intValue()
      val kn = odf.count().intValue()
      
      var imputationPlans = List.empty[(String, Double, Double, ImputationPlan)]
      
      //val missingRate = Seq(10d, 20d, 30d)
      val missingRate = Seq(20d)
      
      //val selectionReduction = Seq(10d, 20d, 30d)
      val selectionReduction = Seq(10d)
      
      features.foreach(feat => {
        
        feature = feat
        odf = odf.withColumn("originalValue", col(feature))
        
        missingRate.foreach(mr => {
          
          val idf = new Eraser().run(odf, feature, mr)
          
          selectionReduction.foreach(sr => {
            
            
            // selection -> clustering -> regression
            
            var impPlanAvg = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)
            var impPlanKnn = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)
            var imputationParamsAvg: HashMap[String, Any] = HashMap()
            var imputationParamsKnn: HashMap[String, Any] = HashMap(
            "k" -> 2,
            "kLimit" -> kn)
            
            /*
            var selectionParamsAvg: HashMap[String, Any] = HashMap(
            "percentReduction" -> sr)
            
            var selectionParamsKnn: HashMap[String, Any] = HashMap(
            "percentReduction" -> sr)
            
            impPlanAvg.addStrategy(new SelectionStrategy(selectionParamsAvg, new Pca()))
            impPlanKnn.addStrategy(new SelectionStrategy(selectionParamsKnn, new Pca()))
            
            var clusteringParamsAvg: HashMap[String, Any] = HashMap(
            "k" -> 2, 
            "maxIter" -> 1000, 
            "kLimit" -> kn)
            
            var clusteringParamsKnn: HashMap[String, Any] = HashMap(
            "k" -> 2, 
            "maxIter" -> 1000, 
            "kLimit" -> kn)
            
            impPlanAvg.addStrategy(new ClusteringStrategy(clusteringParamsAvg, new KMeansPlus()))
            impPlanKnn.addStrategy(new ClusteringStrategy(clusteringParamsKnn, new KMeansPlus()))
            
            var imputationParamsAvg: HashMap[String, Any] = HashMap()
            
            impPlanAvg.addStrategy(new ImputationStrategy(imputationParamsAvg, new Avg()))
            
            imputationPlans = imputationPlans :+ (feature, mr, sr, impPlanAvg)
            
            var imputationParamsKnn: HashMap[String, Any] = HashMap(
            "k" -> 2,
            "kLimit" -> kn)
            
            impPlanKnn.addStrategy(new ImputationStrategy(imputationParamsKnn, new Knn()))
            
            imputationPlans = imputationPlans :+ (feature, mr, sr, impPlanKnn)
            
            
            
            // clustering -> selection -> regression
            
            impPlanAvg = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)
            impPlanKnn = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)
            
            clusteringParamsAvg = HashMap(
            "k" -> 2, 
            "maxIter" -> 1000, 
            "kLimit" -> kn)
            
            clusteringParamsKnn = HashMap(
            "k" -> 2, 
            "maxIter" -> 1000, 
            "kLimit" -> kn)
            
            impPlanAvg.addStrategy(new ClusteringStrategy(clusteringParamsAvg, new KMeansPlus()))
            impPlanKnn.addStrategy(new ClusteringStrategy(clusteringParamsKnn, new KMeansPlus()))
            
            selectionParamsAvg = HashMap(
            "percentReduction" -> sr)
            
            selectionParamsKnn = HashMap(
            "percentReduction" -> sr)
            
            impPlanAvg.addStrategy(new SelectionStrategy(selectionParamsAvg, new Pca()))
            impPlanKnn.addStrategy(new SelectionStrategy(selectionParamsKnn, new Pca()))
            
            imputationParamsAvg = HashMap()
            
            impPlanAvg.addStrategy(new ImputationStrategy(imputationParamsAvg, new Avg()))
            
            //imputationPlans = imputationPlans :+ impPlanAvg
            
            imputationParamsKnn = HashMap(
            "k" -> 2,
            "kLimit" -> kn)
            
            impPlanKnn.addStrategy(new ImputationStrategy(imputationParamsKnn, new Knn()))
            
            imputationPlans = imputationPlans :+ (feature, mr, sr, impPlanKnn)
            
            
            
            // clustering -> regression
            
            impPlanAvg = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)
            impPlanKnn = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)
            
            clusteringParamsAvg = HashMap(
            "k" -> 2, 
            "maxIter" -> 1000, 
            "kLimit" -> kn)
            
            clusteringParamsKnn = HashMap(
            "k" -> 2, 
            "maxIter" -> 1000, 
            "kLimit" -> kn)
            
            impPlanAvg.addStrategy(new ClusteringStrategy(clusteringParamsAvg, new KMeansPlus()))
            impPlanKnn.addStrategy(new ClusteringStrategy(clusteringParamsKnn, new KMeansPlus()))
            
            imputationParamsAvg = HashMap()
            
            impPlanAvg.addStrategy(new ImputationStrategy(imputationParamsAvg, new Avg()))
            
            imputationPlans = imputationPlans :+ (feature, mr, sr, impPlanAvg)
            
            imputationParamsKnn = HashMap(
            "k" -> 2,
            "kLimit" -> kn)
            
            impPlanKnn.addStrategy(new ImputationStrategy(imputationParamsKnn, new Knn()))
            
            imputationPlans = imputationPlans :+ (feature, mr, sr, impPlanKnn)
            
            
            
            // selection -> regression
            
            impPlanAvg = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)
            impPlanKnn = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)
            
            selectionParamsAvg = HashMap(
            "percentReduction" -> sr)
            
            selectionParamsKnn = HashMap(
            "percentReduction" -> sr)
            
            impPlanAvg.addStrategy(new SelectionStrategy(selectionParamsAvg, new Pca()))
            impPlanKnn.addStrategy(new SelectionStrategy(selectionParamsKnn, new Pca()))
            
            imputationParamsAvg = HashMap()
            
            impPlanAvg.addStrategy(new ImputationStrategy(imputationParamsAvg, new Avg()))
            
            //imputationPlans = imputationPlans :+ impPlanAvg
            
            imputationParamsKnn = HashMap(
            "k" -> 2,
            "kLimit" -> kn)
            
            impPlanKnn.addStrategy(new ImputationStrategy(imputationParamsKnn, new Knn()))
            
            imputationPlans = imputationPlans :+ (feature, mr, sr, impPlanKnn)
            
            */
            
            // regression
            
            impPlanAvg = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)
            impPlanKnn = new ImputationPlan(idf, odf, mr, feature, features, parallelExecution)
            
            imputationParamsAvg = HashMap()
            
            impPlanAvg.addStrategy(new ImputationStrategy(imputationParamsAvg, new Avg()))
            
            imputationPlans = imputationPlans :+ (feature, mr, sr, impPlanAvg)
            
            imputationParamsKnn = HashMap(
            "k" -> 2,
            "kLimit" -> kn)
            
            impPlanKnn.addStrategy(new ImputationStrategy(imputationParamsKnn, new Knn()))
            
            imputationPlans = imputationPlans :+ (feature, mr, sr, impPlanKnn)
            
          })
          
        })
        
      })
      
      //val imputationPlansRdd = spark.sparkContext.parallelize(imputationPlans)
      //imputationPlansRdd.map(plan => (plan.planName, plan.run())).foreach(println(_))
      
      var resultList = new Crowner().run(imputationPlans, parallelExecution)
      
      var consResult = new Reviewer().run(resultList, missingRate, selectionReduction)
      
      Logger.getLogger(getClass.getName).error("")
      Logger.getLogger(getClass.getName).error("")
      Logger.getLogger(getClass.getName).error("")
      Logger.getLogger(getClass.getName).error(" ------ CONSOLIDATED RESULT ------ ")
      
      consResult.foreach(x => {
        
        Logger.getLogger(getClass.getName).error("Plan: " + x._1 + "	" + "Missing rate: " + x._2 + "	" 
                                                + "Selection reduction: " + x._3 + "	" + "Error: " + x._4 + "%") 
        
      })
      
      val bestPlan = consResult.sortBy(_._4).head
      
      Logger.getLogger(getClass.getName).error("")
      Logger.getLogger(getClass.getName).error("Best plan: " + bestPlan._1 + "	" + "Missing rate: " + bestPlan._2 + "	" 
                                                + "Selection reduction: " + bestPlan._3 + "	" + "Error: " + bestPlan._4 + "%") 
      
      val wallStopTime = new java.util.Date()
    
      val wallTimeseconds   = ((wallStopTime.getTime - wallStartTime.getTime) / 1000)
      
      val wallTimesMinutes = wallTimeseconds / 60
      
      val wallTimesHours = wallTimesMinutes / 60
      
      Logger.getLogger(getClass.getName).error("------------------ Wall stop time: "
                                              + appraisal.spark.util.Util.getCurrentTime(wallStartTime)
                                              + " --- Total wall time: " + wallTimeseconds + " seconds, "
                                              + wallTimesMinutes + " minutes, "
                                              + wallTimesHours + " hours.")
      
    }catch{
      
      case ex : Exception => {
                              ex.printStackTrace()
                              Logger.getLogger(getClass.getName).error(ex)
      }
      
    }
    
  }
  
}