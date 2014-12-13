

class Vector(val vect:Array[Int]) {
    var data:Array[Int]=vect;
  def this(size:Int){
    this(new Array[Int](size));
  }

  def increment(index:Int){
    data(index)+=1;
  }
  def decrement(index:Int){
    data(index)-=1;
  }
  def printIt(){
    print("[")
    for(i<-0 to data.length-1)print(data(i)+",");
        print("]\n")
  }
  def forEach(callback:(Int) => Unit)={
    for(i<-0 to data.length-1)callback(data(i));
  }
  def add(a:Array[Int]){
    for(i<-0 to data.length-1)data(i)+=a(i);
  }
  def sumAll():Int={
    var sum:Int=0;
    for(i<-0 to data.length-1)sum+=data(i);
    (sum)
  }
  def normalize(){
    var sum:Int=0;
    for(i<-0 to data.length-1)sum+=data(i);
    if (sum>0) {for(i<-0 to data.length-1)data(i)/=sum};
    
  }
  }