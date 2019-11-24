package appraisal.spark.engine

class Reviewer {
  
  def run(resultList: List[(String, Double, Double, Double, String)], missingRate: Seq[Double], selectionReduction: Seq[Double]): List[(String, Double, Double, Double)] = {
    
    val execPlanNames = resultList.map(_._1).distinct
      
    var consResult = List.empty[(String, Double, Double, Double)]
    
    missingRate.foreach(mr => {
      
      selectionReduction.foreach(sr => {
        
        execPlanNames.foreach(planName => {
          
          val conRes = resultList.filter(x => x._1.equals(planName) && x._2 == mr && x._3 == sr)
          
          if(conRes.size > 0){
          
            val count = conRes.size
            val avgPlanError = conRes.map(_._4).reduce(_ + _) / count
            
            consResult = consResult :+ (planName, mr, sr, avgPlanError)
            
          }
          
        })
        
      })
      
    })
    
    consResult
    
  }
  
  def runEnsemble(resultList: List[(String, Double, Double, Int, Double)], missingRate: Seq[Double], selectionReduction: Seq[Double]): List[(String, Double, Double, Double)] = {
   
    val execPlanNames = resultList.map(_._1).distinct
      
    var consResult = List.empty[(String, Double, Double, Double)]
    
    missingRate.foreach(mr => {
      
      selectionReduction.foreach(sr => {
        
        execPlanNames.foreach(planName => {
          
          val conRes = resultList.filter(x => x._1.equals(planName) && x._2 == mr && x._3 == sr)
          
          if(conRes.size > 0){
          
            val count = conRes.size
            val avgPlanError = conRes.map(_._5).reduce(_ + _) / count
            
            consResult = consResult :+ (planName, mr, sr, avgPlanError)
            
          }
          
        })
        
      })
      
    })
    
    consResult
    
  }
  
}