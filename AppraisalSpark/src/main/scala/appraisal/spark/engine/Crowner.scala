package appraisal.spark.engine

import org.apache.log4j._

class Crowner {
  
  def run(imputationPlans: List[(String, Double, Double, ImputationPlan)], parallelExecution: Boolean): List[(String, Double, Double, Double, String)] = {
    
    val planCount = imputationPlans.size
    var qPlan = planCount
    
    var resultList = List.empty[(String, Double, Double, Double, String)]
    
    if(parallelExecution){
      
      imputationPlans.par.foreach(plan => {
        
        var execResult = plan._4.run()
        
        if(execResult != null){
        
          resultList = resultList :+ (plan._4.planName, plan._2, plan._3, execResult.avgPercentError, execResult.params)
        
        }
          
        qPlan -= 1
        val rPlan = planCount - qPlan
        val percC = (100 - ((100 * qPlan) / planCount))
        
        Logger.getLogger(getClass.getName).error("Executed plans: " + rPlan + " / " + planCount + " : " + percC + "%.")
        
      })
      
    }else{
    
      imputationPlans.foreach(plan => {
        
        var execResult = plan._4.run()
        
        if(execResult != null){
        
          resultList = resultList :+ (plan._4.planName, plan._2, plan._3, execResult.avgPercentError, execResult.params)
        
        }
        
        qPlan -= 1
        val rPlan = planCount - qPlan
        val percC = (100 - ((100 * qPlan) / planCount))
        
        Logger.getLogger(getClass.getName).error("Executed plans: " + rPlan + " / " + planCount + " : " + percC + "%.")
        
      })
    
    }
    
    resultList
    
  }
  
  def runEnsemle(imputationPlans: List[(String, Double, Double, Int, ImputationPlan)], parallelExecution: Boolean): List[(String, Double, Double, Int, Double)] = {
    
    val planCount = imputationPlans.size
    var qPlan = planCount
    
    var resultList = List.empty[(String, Double, Double, Int, Double)]
    
    if(parallelExecution){
      
      imputationPlans.par.foreach(plan => {
      
        var execResult = plan._5.run()
        
        if(execResult != null){
        
          resultList = resultList :+ (plan._5.planName, plan._2, plan._3, plan._4, execResult.avgPercentError)
          
        }
        
        qPlan -= 1
        val rPlan = planCount - qPlan
        val percC = (100 - ((100 * qPlan) / planCount))
        
        Logger.getLogger(getClass.getName).error("Executed plans: " + rPlan + " / " + planCount + " : " + percC + "%.")
      
      })
      
    }else{
    
      imputationPlans.foreach(plan => {
        
        var execResult = plan._5.run()
        
        if(execResult != null){
        
          resultList = resultList :+ (plan._5.planName, plan._2, plan._3, plan._4, execResult.avgPercentError)
          
        }
        
        qPlan -= 1
        val rPlan = planCount - qPlan
        val percC = (100 - ((100 * qPlan) / planCount))
        
        Logger.getLogger(getClass.getName).error("Executed plans: " + rPlan + " / " + planCount + " : " + percC + "%.")
        
      })
      
    }
  
    resultList
    
  }
  
}