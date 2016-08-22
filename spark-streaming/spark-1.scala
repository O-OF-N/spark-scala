//import StreamingContext, stuff under it and Seconds.
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.StreamingContext._;
import org.apache.spark.streaming.Seconds;

//create a new StreamingContext.
val ssc = new StreamingContext(sc,Seconds(1));

//This StreamingContext works this way. 
//It listens to a queue. When the queue has something in it,
//it de-queues and performs the required operation.
//If the queue is empty, it just waits.

//Import queue.
import scala.collection.mutable.Queue

//The Queue is of type, RDD of Integers.
//So import RDD.
import org.apache.spark.rdd.RDD

//create a new RDD queue
val rddQueue= new Queue[RDD[Int]]()
//create a stream that reads from the rdd queue
val inputStream = ssc.queueStream(rddQueue);
// Takes the input, finds a modulo
// If an input of 1 to 1000 is gives as input stream, it finds the modulo of each
//so we will have all these values between 0 to 9.
//In addition the count is added as the second parameter of the tuple.
val mappedInputStream = inputStream.map(x=>(x%10,1)); 

//It takes the count,of each number(remember every number is between 0 and 9.
//Gives the total count of each number.
val reducedStream = mappedInputStream.reduceByKey((x,y)=>x+y);
reducedStream.print();
ssc.start();

//Putting in some data into the queue
//Put a stream of 1 to 1000 ten times into the queue at an interval of 1 second
for(i <- 1 to 10){
     rddQueue.synchronized {
        rddQueue += ssc.sparkContext.makeRDD(1 to 1000,10);
     }
     Thread.sleep(1000);
}


ssc.stop();


