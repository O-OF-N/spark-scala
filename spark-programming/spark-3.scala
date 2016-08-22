//mapByKey = groups by key. 
//(a,1),(a,2),(b,1),(c,1)=>groups by a,b,c
//mapValues = does not touch the key. Works only on the values.

//Example: From autoData,we want the average mielage per brand:
//Step 1: Get the mielage by brand.
val cm = autoData.map(cars=>{
    val car= cars.split(','); 
    (car(0),car(7))
});
//Step 2: Remove the header
val header = cm.first();
val cm1 = cm.filter(x=>x!=header);
//Step 3: convert mielage values to int. Also add a count to get the total count in reduce.
val cm1WithVal = cm1.mapValues(x=>(x.toInt,1));
//Step 4: Get the total mielage and count per brand
val totalMielage = cm1WithVal.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2));
//Step 5: Get Average:
totalMielage.mapValues(x=>x._1/x._2);


/**Broadcast variables: These are variables that are effeciently copied by Spark to 
individual nodes. They can be used to keep a copy of look up variables to all nodes
instead of individually sending them to each node */
val sedan = sc.broadcast("sedan");
val hatchback = sc.broadcast("hatchback");
//accessed as :
    sedan.value
    hatchback.value
/**Accumulator variables are used to store counters from across nodes. There will only
be a single counter instance and this will be shared across all nodes. IF one node
increments the counter all nodes will see the incremented value */
val sedanCount = sc.accumulator(0);
val hatchbackCount = sc.accumulator(0);
//incremented as :
sedanCount +=1 
//or
sedanCount.add(1)

//TO get the number of partitions:
RDD.getNumPartitions
//ex:
collData.getNumPartitions

//By default: it takes the number of cores in the machine. How ever, if you want to override it,
sc.paralellalize(Array(1,2,3,4,5),2);