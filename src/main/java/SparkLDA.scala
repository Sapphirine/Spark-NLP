import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import scala.collection.mutable._
import org.apache.spark.AccumulatorParam



object SparkLDA {

	implicit def arrayToVector(s: Array[Int]) = new Vector(s);
	implicit def vectorToArray(s: Vector) = s.data;

	implicit object VectorAP extends AccumulatorParam[Vector] {
		def zero(v: Vector) = new Vector(v.data.size)
		def addInPlace(v1: Vector, v2: Vector):Vector = {
			for (i <- 0 to v1.data.size-1) v1.data(i) += v2.data(i)
					return v1
		}
	} 


	def main(args: Array[String]){
		val numtopics=10;
		lda("hdfs://localhost:8020/README.md","","",numtopics,(50/numtopics),0.1,15,false);
	}

	def lda(pathToFileIn:String,pathToFileOut:String,pathEvalLabels:String,numTopics:Int,alpha:Double,beta:Double,numIter:Int,deBug:Boolean){
		val (conf,sc)=initializeSpark();
		val(documents,dictionary,topicCount)=importText("hdfs://localhost:8020/README.md",numTopics,sc);

		for(i<-0 to numIter){

			//      buildVariables();
			//      broadcast();
			//      step();

		}

	}
	def initializeSpark()={
		val conf = new SparkConf().setAppName("Simple Application");
		val sc = new SparkContext(conf);
		(conf,sc)
	}

	def importText(pathToFileIn:String,numTopics:Int,sc:SparkContext)={

		val stopWords =sc.broadcast(List[String]("a","able","about","above","according","accordingly","across","actually","after","afterwards","again","against","all","allow","allows","almost","alone","along","already","also","although","always","am","among","amongst","an","and","another","any","anybody","anyhow","anyone","anything","anyway","anyways","anywhere","apart","appear","appreciate","appropriate","are","around","as","aside","ask","asking","associated","at","available","away","awfully","b","be","became","because","become","becomes","becoming","been","before","beforehand","behind","being","believe","below","beside","besides","best","better","between","beyond","both","brief","but","by","c","came","can","cannot","cant","cause","causes","certain","certainly","changes","clearly","co","com","come","comes","concerning","consequently","consider","considering","contain","containing","contains","corresponding","could","course","currently","d","definitely","described","despite","did","different","do","does","doing","done","down","downwards","during","e","each","edu","eg","eight","either","else","elsewhere","enough","entirely","especially","et","etc","even","ever","every","everybody","everyone","everything","everywhere","ex","exactly","example","except","f","far","few","fifth","first","five","followed","following","follows","for","former","formerly","forth","four","from","further","furthermore","g","get","gets","getting","given","gives","go","goes","going","gone","got","gotten","greetings","h","had","happens","hardly","has","have","having","he","hello","help","hence","her","here","hereafter","hereby","herein","hereupon","hers","herself","hi","him","himself","his","hither","hopefully","how","howbeit","however","i","ie","if","ignored","immediate","in","inasmuch","inc","indeed","indicate","indicated","indicates","inner","insofar","instead","into","inward","is","it","its","itself","j","just","k","keep","keeps","kept","know","knows","known","l","last","lately","later","latter","latterly","least","less","lest","let","like","liked","likely","little","ll","look","looking","looks","ltd","m","mainly","many","may","maybe","me","mean","meanwhile","merely","might","more","moreover","most","mostly","much","must","my","myself","n","name","namely","nd","near","nearly","necessary","need","needs","neither","never","nevertheless","new","next","nine","no","nobody","non","none","noone","nor","normally","not","nothing","novel","now","nowhere","o","obviously","of","off","often","oh","ok","okay","old","on","once","one","ones","only","onto","or","other","others","otherwise","ought","our","ours","ourselves","out","outside","over","overall","own","p","particular","particularly","per","perhaps","placed","please","plus","possible","presumably","probably","provides","q","que","quite","qv","r","rather","rd","re","really","reasonably","regarding","regardless","regards","relatively","respectively","right","s","said","same","saw","say","saying","says","second","secondly","see","seeing","seem","seemed","seeming","seems","seen","self","selves","sensible","sent","serious","seriously","seven","several","shall","she","should","since","six","so","some","somebody","somehow","someone","something","sometime","sometimes","somewhat","somewhere","soon","sorry","specified","specify","specifying","still","sub","such","sup","sure","t","take","taken","tell","tends","th","than","thank","thanks","thanx","that","thats","the","their","theirs","them","themselves","then","thence","there","thereafter","thereby","therefore","therein","theres","thereupon","these","they","think","third","this","thorough","thoroughly","those","though","three","through","throughout","thru","thus","to","together","too","took","toward","towards","tried","tries","truly","try","trying","twice","two","u","un","under","unfortunately","unless","unlikely","until","unto","up","upon","us","use","used","useful","uses","using","usually","uucp","v","value","various","ve","very","via","viz","vs","w","want","wants","was","way","we","welcome","well","went","were","what","whatever","when","whence","whenever","where","whereafter","whereas","whereby","wherein","whereupon","wherever","whether","which","while","whither","who","whoever","whole","whom","whose","why","will","willing","wish","with","within","without","wonder","would","would","x","y","yes","yet","you","your","yours","yourself","yourselves","z","zero"));
		val textFile=sc.textFile(pathToFileIn).cache();
		val documents=textFile.map(line=>{
			val topicDistrib=new Array[Int](numTopics);
			val lineCleaned=line.replaceAll("[^A-Za-z ]","").toLowerCase();
			(lineCleaned.split(" ").map(word=>{
				var topic:Int=0
						var wrd:String="";
			if(word.length()>1&&(!stopWords.value.contains(word))){
				topic =Integer.parseInt(Math.round(Math.random()*(numTopics-1)).toString);
				topicDistrib.increment(topic);
				wrd=word;
			}
			(wrd,topic);
			}),topicDistrib)
		});
		val(dictionary,topicCount)=updateVariables(documents,numTopics);
		(documents,dictionary,topicCount)
	}
  
  def updateVariables(documents:RDD[(Array[(String, Int)], Array[Int])],numTopics:Int)={
    val dictionary=documents.flatMap(line=>line._1).map(tuple=>{
      var value:Array[Int]=new Array[Int](numTopics);
      if(!tuple._1.equals("")){
        value(tuple._2)+=1;
      }
      (tuple._1,value)
    }).reduceByKey((a:Array[Int],b)=>{
      for(i<-0 to a.length-1){
        a(i)+=b(i);
      }
      (a);
    }).collect().toMap;
    val topicCount:Array[Int]=new Array[Int](numTopics);
    dictionary.foreach(t=>topicCount.add(t._2));
    (dictionary,topicCount)
  }

	def printMetrix(){

	}

	def saveResult{

	}

	def gibbsSampling(docTopicDistrib:Array[Int],wordTopicDistrib:Array[Int],topicCount:Array[Int],alpha:Double,beta:Double,v:Int):Int={
			val numTopic=docTopicDistrib.length;
			var ro:Array[Double]=Array[Double](numTopic);
			ro(0)=(docTopicDistrib(0)+alpha)*(wordTopicDistrib(0)+beta)/(topicCount(0)+v*beta);
			for(i<-1 to numTopic-1){
				ro(i)=ro(i-1)+(docTopicDistrib(0)+alpha)*(wordTopicDistrib(0)+beta)/(topicCount(0)+v*beta);
			}
			var x=Math.random()*ro(numTopic-1);
			var i:Int=0;
			while(x>ro(i))i+=1;
			return i;
	}









}



