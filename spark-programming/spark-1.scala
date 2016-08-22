// In the console, spark context is available as "sc".
// Simple Array

/**
=> Creates a basic RDD. It takes the array as input and distributes it over the available nodes.
=> RDD creation is lazy. So basically executing this command will not create the RDD. 
   The RDD is created only when an action is executed.
*/
val collData = sc.parallelize(Array(1,2,3,4,5)); 

/**
=> This dataset can be added to an in-memory cache, which will be available across multiple nodes.
=> This is mostly used in cases where a repetitive dataset has to be used.
*/
collData.cache();

/**
=> Executing count action. 
=> Only when an action is executed, the operations like RDD creation and transformations are executed.
=> Count is an action.
*/
collData.count();

/********************************************************************************************************/


// Load from a text file.

/**
=> RDD creation
=> datadir is a variable that is already created. And it points to the directory containing dataset
*/
val datadir = "/Users/vinodm1986/Workspace/spark/data";
val autoData = sc.textFile(datadir+'/auto-data.csv');
/********************************************************************************************************/

// cache it 
autoData.cache();

// Get count 
autoData.count();

// Other operations
// Get the first element
autoData.first();

// To get first 'n' elements.
autoData.take(5);

//convert RDD to array
autoData.collect();

//foreach
autoData.foreach((element)=>{println(element)});

//for
for(x <- autoData.collect) {println(x)};
/********************************************************************************************************/

/********Transformations ********/
/**
=> map transformation. This is just like normal map.
=> This will create a new RDD.
=> Due to lazy evaluation, this will not be executed unless an action is called on the RDD.
=> Transformations can be chained.
*/
val tabseperated = autoData.map(element => element.replace(",","\t"));
//calling an Action on the new RDD
tabseperated.count();
tabseperated.take(5);

// Filter transformation. Similar to every filter operation.
val toyotaData = autoData.filter(data => data.contains("toyota"));
toyota.take(5);

//flatmap transformation is again similar to flatmap in every other language.
val word = tsvData.flatMap(x => x.split("\t"));
word.take(20);

//distinct
val distinctWord = word.distinct;

/** Set operations */
val a = sc.parallelize(Array(1,2,3,4,5));
val b = sc.parallelize(Array(5,6,7,8,9));
// Union => Gives the union of 2 RDDS. it does not eliminate duplicates
a.union(b)
//use distinct to eliminate duplicates
a.union(b).distinct.take(10);

// Intersection => Gives the common elements between two RDDS
a.intersection(b).take(10);
/********************************************************************************************************/
