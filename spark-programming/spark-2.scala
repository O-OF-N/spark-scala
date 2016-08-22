//collData = Array(1,2,3,4,5)
// Actions

//reduce => Same as reduce in other languages. No difference.
collData.reduce((x,y)=>x+y);
autoData.reduce((l1,l2)=> if(l1.length() < l2.length) l1 else l2);


//Aggregate
/**
=>The aggregate operation is used to perform more than one reduce operations.
=> It is a collection of 2 different operations viz. Sequence and Combiner
 * Sequence => acts on individual partition
 * Combiner => acts on the result from individual partitions. It acts across partitions.
*/

/**
 For example:
 let us do the same reduce operation using aggregation.
 */

 def seq(x:Int, y:Int):Int = {println(x);println(y);x+y};
 def comb(x:Int, y:Int):Int = {println(x);println(y);x+y};
 
collData.aggregate(0)(seq,comb);

/** The 0 provided initially is the radix value. Same as the second parameter of 
reduce function in JS.
As you can see, seq and comb are one and the same functions.
I have created them as two different functions only for ease of explanation.
seq: x=0,y=1,x=0,y=2,x=0,y=3,x=0,y=4,x=0,y=5. It is basically applied per partition.
It supplies y to comb operation as (1,2,3,4,5)
comb: x = 0,y=1,x=1,y=2,x=3,y=3,x=6,y=4,x=10,y=5;
It takes input of every partition and adds it.
*/

// Another example: Takes collData and returns a tuple, which will have the sum and product 
// of all numbers in it.
collData.aggregate(0,1)((x,y)=>(x._1+y,x._2*y), (x,y)=>(x._1+y._1,x._2*y._2));
// Okay, here is how this works:
/** for the sequence function(for each partition): 
        * x is a tuple (0,1)
        * y is the array (1,2,3,4,5)
        * returns : 0+1 = 1, 1*1 = 1
                    0+2 = 2, 1*2 = 2
                    0+3 = 3, 1*3 = 3
                    0+4 = 4, 1*4 = 4
                    0+5 = 5, 1*5 = 5
                    => (1,1),(2,2),(3,3),(4,4),(5,5)
for the combiner function:
    x takes seed value for first iteration and then the previous result for subsequest iterations
    y takes the collection of tuples returned by the sequence function.
    * itr1: x = (0,1) y = (1,1) returns : 0+1,1*1 = (1,1)
    * itr2: x = (1,1) y = (2,2) returns : 1+2,1*2 = (3,2)
    * itr3: x = (3,2) y = (3,3) returns : 3+3,2*3 = (6,6)
    * itr4: x = (6,6) y = (4,4) returns : 6+4,6*4 = (10,24)
    * itr5: x = (10,24) y = (5,5) returns : 10+5,24*5 = (15,120)

Final result: After these 5 iterations of combiner, tuple (15,20) is returned as output.
// If needed to see output at the end of every iteration,use:
a.aggregate(0,1)((x,y)=>{println("x1 = "+ x._1);println("y1 = "+ y);(x._1+y,x._2*y)}, (x,y)=>{println("x11 = "+ x._1);println("y11 = "+ y._1); println("x12 = "+ x._2);println("y12 = "+ y._2);(x._1+y._1,x._2*y._2)});
*/

// Functions:

// Just demostrates function can be used in map
def map(data:String):String = {
    val elements = data.split(',');
    elements(3) = if (elements(3).equals("two")) "2" else  "4";//replaces "four"=>"4","two"=>"2"
    elements(3);//just to println on console.
    elements(5) = elements(5).toUpperCase();//upper cases
    elements.mkString(":");//same as join(':') in js
};

autoData.map(map);

//Map-reduce -1
//Find the average mileage of all cars. 
//Mileage is the 10th element.
def map (x:String):Int = {
     val mile:String = x.split(",")(9);
     if(mile.matches("^\\d*$")) mile.toInt
     else 0
};
def reduce(x:Int,y:Int):Int = x+y;
autoData.map(map).reduce(reduce);//4955
autoData.count; //198

//Map reduce-2
//Automatially find the mileage(i.e calculate total mileage and number of cars in one step
//using tuples).
def map(x:String):(Int,Int) = {
    val mile:String = x.split(",")(9);
    if(mile.matches("^\\d*$")) (mile.toInt,1);
    else (0,1);
}; //Returns a tuple with the mileage value and 1(which is the count of one car);

def reduce(x:(Int,Int),y:(Int,Int)):(Int,Int) = (x._1+y._1,x._2,y._2);
//sums up the mileage and the count and returns a tuple with both elements.
val mile = autoData.map(map).reduce(reduce);//(4955,198)
val ave = mile._1/mile._2;
