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
    (conf,sc)
  }
  
  def importText(pathToFileIn:String){
    /* 
     * creates a wordcount for each line of the input file as RDD and dictionnary for all documents as a map
     * Filter stop words
     */
    val documents=null
    val dictionnary=null
    
    (documents,dictionnary)
  }
  
  def initializeTopicDistribution(documents:RDD[String]){
    /*
     * creates a document RDD with for each line each word and a random topic ex: (obama,1)(obama,2)(table,1) ...
     * for each line in the documents, for each word in the line, for the number of time the word appears, pick a random topic
     * build a string 
     */
  }
  
  def printMetrix(){
    
  }
  
  def saveResult{
    
  }
  
}