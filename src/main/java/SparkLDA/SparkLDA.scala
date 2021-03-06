package SparkLDA

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import scala.collection._
import scala.collection.mutable._
import scala.collection.immutable._
import org.apache.spark.AccumulatorParam
import java.io._
import org.apache.commons.math3.special._;
import org.apache.log4j.Logger
import org.apache.log4j.Level


object SparkLDA {

	implicit def arrayToVector(s: Array[Int]) = new Vector(s);
	implicit def vectorToArray(s: Vector) = s.data;



	def main(args: Array[String]){


		var numTopics:Int=0;
	var inPath:String="";
	var outPath:String="";
	var master:String="local[*]";
	var iter:Int=0;
  var mem:String="1g";
	var debug=false;
  try{
  	for(i<-0 to args.length-1){
  		if(args(i).equals("-nt")) numTopics=Integer.parseInt(args(i+1));
  		if(args(i).equals("-in"))inPath=args(i+1);
  		if(args(i).equals("-out"))outPath=args(i+1);
  		if(args(i).equals("-m"))master=args(i+1);
      if(args(i).equals("-mem"))mem=args(i+1);
  		if(args(i).equals("-iter"))iter=Integer.parseInt(args(i+1));
  		if(args(i).equals("-deb"))debug=true;
  	}
  }catch {
    case e: Exception => {System.err.println("Please use -nt NumTopics -in InpuPath -out OutputPath -m MasterURL (default = local) -mem spark.executor.memory (default 1Bg) -iter NumIter -deb Debug (default = false)");}
    return;
  }
  
  if(numTopics==0||inPath.equals("")||outPath.equals("")||iter==0){System.err.println(numTopics+" "+inPath+" "+outPath+" "+iter+"Not enough input arguments. Please use -nt NumTopics -in InpuPath -out OutputPath -m MasterURL (default = local) -mem spark.executor.memory (default 1Bg) -iter NumIter -deb Debug (default = false)"); return;}

	lda(inPath,outPath,master,numTopics,(50/numTopics),0.1,iter,debug,mem);
	}

	def lda(pathToFileIn:String,pathToFileOut:String,URL:String,numTopics:Int,alpha:Double,beta:Double,numIter:Int,deBug:Boolean,mem:String){


		val (conf,sc)=initializeSpark(URL,deBug,mem);
		var(documents,dictionary,topicCount)=importText(pathToFileIn,numTopics,sc);
		println("------ Done with initialization ------");

		val ll:MutableList[Double]= MutableList[Double]();
		for(i<-0 to numIter){
			println("------ Iteration "+i+" ------");
			var (doc,dict,tC)=step(sc,documents,numTopics,dictionary,topicCount,alpha,beta);
			documents=doc;
			dictionary=dict;
			topicCount=tC;
			if(deBug)ll+=logLikelihood(dictionary,topicCount,alpha,beta);
			System.gc();
		}
		println("------ Saving ------");
		saveAll(documents,ll,sc,dictionary,topicCount,pathToFileOut,deBug);
	}
	def initializeSpark(URL:String,debug:Boolean,mem:String)={
		if(!debug)Logger.getLogger("org").setLevel(Level.WARN);

		val conf = new SparkConf()
		.setAppName("Spark LDA")
		.setMaster(URL)
//    .set("spark.storage.memoryFraction","0")
		.set("spark.executor.memory", mem);

		val sc = new SparkContext(conf);
		(conf,sc)
	}

	def importText(pathToFileIn:String,numTopics:Int,sc:SparkContext)={

		val stopWords =sc.broadcast(List[String]("a","able","about","above","according","accordingly","across","actually","after","afterwards","again","against","all","allow","allows","almost","alone","along","already","also","although","always","am","among","amongst","an","and","another","any","anybody","anyhow","anyone","anything","anyway","anyways","anywhere","apart","appear","appreciate","appropriate","are","around","as","aside","ask","asking","associated","at","available","away","awfully","b","be","became","because","become","becomes","becoming","been","before","beforehand","behind","being","believe","below","beside","besides","best","better","between","beyond","both","brief","but","by","c","came","can","cannot","cant","cause","causes","certain","certainly","changes","clearly","co","com","come","comes","concerning","consequently","consider","considering","contain","containing","contains","corresponding","could","course","currently","d","definitely","described","despite","did","different","do","does","doing","done","down","downwards","during","e","each","edu","eg","eight","either","else","elsewhere","enough","entirely","especially","et","etc","even","ever","every","everybody","everyone","everything","everywhere","ex","exactly","example","except","f","far","few","fifth","first","five","followed","following","follows","for","former","formerly","forth","four","from","further","furthermore","g","get","gets","getting","given","gives","go","goes","going","gone","got","gotten","greetings","h","had","happens","hardly","has","have","having","he","hello","help","hence","her","here","hereafter","hereby","herein","hereupon","hers","herself","hi","him","himself","his","hither","hopefully","how","howbeit","however","i","ie","if","ignored","immediate","in","inasmuch","inc","indeed","indicate","indicated","indicates","inner","insofar","instead","into","inward","is","it","its","itself","j","just","k","keep","keeps","kept","know","knows","known","l","last","lately","later","latter","latterly","least","less","lest","let","like","liked","likely","little","ll","look","looking","looks","ltd","m","mainly","many","may","maybe","me","mean","meanwhile","merely","might","more","moreover","most","mostly","much","must","my","myself","n","name","namely","nd","near","nearly","necessary","need","needs","neither","never","nevertheless","new","next","nine","no","nobody","non","none","noone","nor","normally","not","nothing","novel","now","nowhere","o","obviously","of","off","often","oh","ok","okay","old","on","once","one","ones","only","onto","or","other","others","otherwise","ought","our","ours","ourselves","out","outside","over","overall","own","p","particular","particularly","per","perhaps","placed","please","plus","possible","presumably","probably","provides","q","que","quite","qv","r","rather","rd","re","really","reasonably","regarding","regardless","regards","relatively","respectively","right","s","said","same","saw","say","saying","says","second","secondly","see","seeing","seem","seemed","seeming","seems","seen","self","selves","sensible","sent","serious","seriously","seven","several","shall","she","should","since","six","so","some","somebody","somehow","someone","something","sometime","sometimes","somewhat","somewhere","soon","sorry","specified","specify","specifying","still","sub","such","sup","sure","t","take","taken","tell","tends","th","than","thank","thanks","thanx","that","thats","the","their","theirs","them","themselves","then","thence","there","thereafter","thereby","therefore","therein","theres","thereupon","these","they","think","third","this","thorough","thoroughly","those","though","three","through","throughout","thru","thus","to","together","too","took","toward","towards","tried","tries","truly","try","trying","twice","two","u","un","under","unfortunately","unless","unlikely","until","unto","up","upon","us","use","used","useful","uses","using","usually","uucp","v","value","various","ve","very","via","viz","vs","w","want","wants","was","way","we","welcome","well","went","were","what","whatever","when","whence","whenever","where","whereafter","whereas","whereby","wherein","whereupon","wherever","whether","which","while","whither","who","whoever","whole","whom","whose","why","will","willing","wish","with","within","without","wonder","would","would","x","y","yes","yet","you","your","yours","yourself","yourselves","z","zero"));
		val textFile=sc.textFile(pathToFileIn);
		val documents=textFile.map(line=>{
			val topicDistrib=new Array[Int](numTopics);
			val lineCleaned=line.replaceAll("[^A-Za-z ]","").toLowerCase();
			(lineCleaned.split(" ").map(word=>{
				var topic:Int=0;
			var wrd:String="";
			if(word.length()>1&&(!stopWords.value.contains(word))){
				topic =Integer.parseInt(Math.round(Math.random()*(numTopics-1)).toString);
				topicDistrib.increment(topic);
				wrd=word;
			}
			(wrd,topic);
			})
			,topicDistrib)
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

	def step(sc:SparkContext,documents:RDD[(Array[(String, Int)], Array[Int])],numTopics:Int,dict:scala.collection.immutable.Map[String, Array[Int]],tC: Array[Int],alpha:Double,beta:Double)={

		val dictionary=sc.broadcast(dict);
		val topicCount=sc.broadcast(tC);
		val v=dict.size;

		val doc=documents.map(tuple=>{
			val topicDistrib=tuple._2
					val line=tuple._1;
			val lineupDated=line.map(t=>{
				val word=t._1;
				var top=t._2;
				if(!t._1.equals("")){
					topicDistrib.decrement(top);
					top=gibbsSampling(topicDistrib,dictionary.value(word),topicCount.value,alpha,beta,v);
					topicDistrib.increment(top);
				}
				(word,top)
			})
			(lineupDated,topicDistrib)
		});
		
		val(dicti,topC)=updateVariables(doc,numTopics);
		(doc:RDD[(Array[(String, Int)], Array[Int])],dicti,topC)

	}
	def printMetrix(){

	}

	def saveAll(documents: RDD[(Array[(String, Int)], Array[Int])],LogLikelihood:MutableList[Double],sc: SparkContext, dictionary: scala.collection.immutable.Map[String, Array[Int]], topicCount: Array[Int],path: String,deBug:Boolean){
		removeAll(path);
		saveDocuments(documents,path);
		saveDictionary(sc,dictionary,path);
		saveTopicCount(sc,topicCount,path);
		if(deBug)saveLogLikelihood (sc,LogLikelihood, path);
	}

	def saveDocuments (documents: RDD[(Array[(String, Int)], Array[Int])], path: String) {
		removeAll(path+"/documentsTopics");  
		documents.map {
		case (topicAssign, topicDist) =>
		var topicDistNorm:Array[Double] = topicDist.normalize();
		val probabilities = topicDistNorm.toList.mkString(", ") 
				(probabilities)
		}.saveAsTextFile(path+"/documentsTopics")
	}
	def saveDictionary(sc: SparkContext, dictionary: scala.collection.immutable.Map[String, Array[Int]], path: String) {
		removeAll(path+"/wordsTopics");
		val dictionaryArray = dictionary.toArray
				val temp = sc.parallelize(dictionaryArray).map {
				case (word, topics) =>
				var topicsNorm:Array[Double] = topics.normalize();
				val topArray = topicsNorm.toList.mkString(", ") 
						val wordCount = topics.sumAll()
						val temp2 = List(word, wordCount, topArray).mkString("\t")
						(temp2)
		}
		temp.saveAsTextFile(path+"/wordsTopics")
	}

	def saveTopicCount (sc: SparkContext, topicCount: Array[Int], path: String) {
		removeAll(path+"/topicCount");
		val temp = sc.parallelize(topicCount).map {
		case (count) =>
		(count)
		}
		temp.saveAsTextFile(path+"/topicCount")
	}
	def saveLogLikelihood (sc: SparkContext,LogLikelihood:MutableList[Double], path: String) {
		removeAll(path+"/logLikelihood");
		val temp = sc.parallelize(LogLikelihood).map {
		case (count) =>
		(count)
		}
		temp.saveAsTextFile(path+"/logLikelihood")
	}

	def gibbsSampling(docTopicDistrib:Array[Int],wordTopicDistrib:Array[Int],topicCount:Array[Int],alpha:Double,beta:Double,v:Int):Int={
			val numTopic=docTopicDistrib.length;
			var ro:Array[Double]=new Array[Double](numTopic);
			ro(0)=(docTopicDistrib(0)+alpha)*(wordTopicDistrib(0)+beta)/(topicCount(0)+v*beta);
			for(i<-1 to numTopic-1){
				ro(i)=ro(i-1)+(docTopicDistrib(i)+alpha)*(wordTopicDistrib(i)+beta)/(topicCount(i)+v*beta);
			}
			var x=Math.random()*ro(numTopic-1);
			var i:Int=0;
			while(x>ro(i)&&i<numTopic-1)i+=1;
			return i;
	}

	def logLikelihood(dictionary: scala.collection.immutable.Map[String, Array[Int]],topicCount:Array[Int],alpha:Double,beta:Double):Double={
			val V:Int=dictionary.size;
	val numTopics:Int=topicCount.length-1;
	var logLikelihood:Double=numTopics*(Gamma.logGamma(V*beta)-V*Gamma.logGamma(beta));

	for (i<-0 to numTopics){
		var sum:Double=0;
	dictionary.foreach{t=>
	sum+=Gamma.logGamma(t._2(i)+beta);
	}
	logLikelihood+=sum-Gamma.logGamma(topicCount(i)+V*beta);
	}

	(logLikelihood)
	}

	def removeAll(pathDir: String) = {
		def delete(file: File): Array[(String, Boolean)] = {
				Option(file.listFiles).map(_.flatMap(f => delete(f))).getOrElse(Array()) :+ (file.getPath -> file.delete)
		}
	}

}



