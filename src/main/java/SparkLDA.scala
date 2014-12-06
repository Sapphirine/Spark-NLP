import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object SparkLDA {

  def main(args: Array[String]) {
    
  }
  
  def lda(pathToFileIn:String,pathToFileOut:String,pathEvalLabels:String,numTopics:Int,alpha:Double,beta:Double,numIter:Int,deBug:Boolean){
    
  }
    
  def initializeSpark{
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
  }
  
  def importText(){
    
  }
  
  def printMetrix(){
    
  }
  
  def saveResult{
    
  }
  
}