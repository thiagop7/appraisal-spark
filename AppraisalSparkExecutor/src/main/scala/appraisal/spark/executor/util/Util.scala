package appraisal.spark.executor.util

import org.apache.spark.sql._

object Util {

  val diabetes = Array[String](    
    "Pregnancies",
    "Glucose",
    "BloodPressure",
    "SkinThickness",
    "Insulin",
    "DiabetesPedigreeFunction",
    "BMI",
    "Age")
  //"Outcome")

  //"class")

  val breastcancer_features = Array[String](
    //"code_number",
    "single_epithelial_cell_size",
    "bland_chromatin",
    "bare_nuclei",
    "mitoses",
    "normal_nucleoli",
    "clump_thickness",
    "uniformity_of_cell_size",
    "uniformity_of_cell_shape",
    "marginal_adhesion")
  //"class")

  //val aidsocurrence_features = Array[String]( "AIDS_HIV" )

  val aidsocurrence_features = Array[String](
    //"Time",
    "AIDS_Death",
    "Regulation_on_the_Prevention_and_Treatment_of_AIDS",
    "AIDS_prevention_knowledge",
    "AIDS_awareness",
    "Handwritten_AIDS_newspaper",
    "Handwritten_anti_AIDS_newspaper",
    "AIDS_virus",
    "How_to_prevent_AIDS_A",
    "Route_of_transmission_of_AIDS",
    "AIDS_prevention",
    "What_is_AIDS",
    "Which_day_is_World_AIDS_Day",
    "The_origins_of_the_AIDS_A",
    "The_origins_of_the_AIDS_B",
    "AIDS",
    "AIDS_Day",
    "The_origins_of_AIDS_C",
    "How_to_prevent_AIDS_B",
    "AIDS_awareness_day",
    "World_AIDS_Day",
    "How_to_prevent_AIDS_C",
    "AIDS_awareness_slogan",
    "AIDS_HIV",
    "Initial_symptoms_of_AIDS",
    "Images_of_AIDS_skin_rashes",
    "How_long_will_one_survive_once_he_she_contracts_HIV",
    "How_long_can_AIDS_patients_survive",
    "HIV_AIDS_prevention",
    "AIDS_village_in_Henan_province",
    "How_does_one_contract_HIV",
    "Number_of_AIDS_patients_in_China",
    "Symptoms_of_AIDS_in_incubation_period")

  val redshift = Array[String](
    "u",
    "g",
    "r",
    "i",
    "z",
    "modelmagerr_u",
    "modelmagerr_g",
    "modelmagerr_r",
    "modelmagerr_i",
    "modelmagerr_z",
    "redshift")

  def loadBreastCancer(spark: SparkSession): DataFrame = {

    //spark.read.option("header", true).csv("C:\\data\\breast-cancer-wisconsin.csv")
    //spark.read.option("header", true).csv("file:///home/oem/Workspace/mestrado/appraisalAdaboost-spark/data/breast-cancer-wisconsin.data.csv")
    spark.read.option("header", true).csv("file:///home/oem/Workspace/mestrado/appraisalAdaboost-spark/data/breast-cancer-wisconsin.reduced.csv")
  }

  def loadAidsOccurenceAndDeath(spark: SparkSession): DataFrame = {

    spark.read.option("header", true).csv("file:///home/oem/Workspace/mestrado/appraisalAdaboost-spark/data/AIDS_Occurrence_and_Death_and_Queries.csv")

  }

  def loadDiabetes(spark: SparkSession): DataFrame = {

    spark.read.option("header", true).csv("file:///home/oem/Workspace/mestrado/appraisalAdaboost-spark/data/diabetes_normalized.csv")

  }
  
  
  def loadData(spark: SparkSession, filePath: String): DataFrame = {

    spark.read.option("header", true).csv(filePath)

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
    case s: String => s.toDouble
  }

}