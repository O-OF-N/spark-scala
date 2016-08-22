/** Create Sql context:
Like spark context, SparkSqlContext is not implicitly available.
It has to be created from sparkcontext as follows:
*/
val sqlContext = new org.apache.spark.sql.SQLContext(sc);
//the implicits is supposed to have some helper functions.
import sqlContext.implicits._;
//Spark sql context works on dataframes. This is different from sparkContext,
//which works on RDDs.
//Read a json: 
//This is a little different from reading a text file using sc.paralellize
val empDf=sqlContext.read.json(dataDir+"customerData.json");

//show:To see the data elements in the Dataframe
empDf.show();

//printSchema: To print the schema
empDf.printSchema;

//To select specific columns in the dataframe,use the select method.
//Select can take comma seperated strings which represent the column names.
empDf.select("age","deptid").show();

//The show method shows all the elements in the schema by default.
//However, if you want to narrow down the search like select top 10 *:
empDf.select("age","deptid").show(2);

//filter: This is like the where clause.
empDf.filter(empDf("age") === 40).show;//note the ===
//filter returns a Dataframe, just like select. So they can be chained if required.
empDf.select("age","deptid").filter(empDf("age") === 40).show(2);
//All relational and logical operators work on filter: && || ! etc.
empDf.select("age","deptid").filter(empDf("age")>10 && empDf("deptid")===100).show(2);


//groupBy: Similar to sql groupBy or spark mapValues+reduceByKey
//As expected, groupBy gives a dataframe.
empDf.groupBy("gender").count.show

//agg: it is a aggregate that can be run on the dataframe returned by groupBy.
//It is used to run different mathematical operations on different columns, unlike only 
//count as in previous case.
empDf.groupBy("gender").agg(max("age"),max("salary")).show
empDf.groupBy("gender").agg(max("age"),avg("salary")).show

//Creating a Dataframe from RDD
//Create two json objects:
val jsonString1:String = "{'name':'dept1','id':100}";
val jsonString2:String = "{'name':'dept2','id':200}";

//Add them to the list:
val deptList = Array(jsonString1,jsonString2);
//Create an RDD with the list:
val deptRDD = sc.parallelize(deptList);
//Create a Dataframe from RDD:
val deptDf = sqlContext.read.json(deptRDD);

//join the two dataframes.
empDf.join(deptDf,empDf("deptid")===deptDf("id")).show;

//combining all of this
empDf.filter(empDf("age")>30) //filter
.join(deptDf,empDf("deptid") === deptDf("id")) //join the result
.groupBy("gender")//group by gender
.agg(avg("age"),max("salary"))//find average age and max of salary
.show;

// an sqlcontext dataframe can be registered as a table.
//doing so, helps us run sql queries directly on the table.
//this can be done by executing "registerTempTable" command on the dataframe.
//registerTempTable takes a mandatory parameter. which is the name of the table.

//for eg. registering empDf as temp table:

empDf.registerTempTable("empl");
//running a query on it:

sqlContext.sql("select * from empl where age>40").show();

//TODO: Connecting to a database.
//JDBC
//Needs the JDBC driver to run on the java path

//Imp: Spark doesnt come built in with a lot of features to save data 
// into the DB. Use Java or corresponding programming language related features.

//Converting RDD to DF.
//use the toDF method. => Important to note that, toDF takes only an array of tuples. 
//It cannot take an array of strings.
//So it the strings have to be converted to tuples before being fed into the toDF method.

//example:
val autoRDD = sc.textFile(datadir+"/auto-data.csv");

val rdd = autoHeaderlessRDD
.map(x=>x.split(','))//split by comma
.map(x => (x(0),x(4),x(7)));//convert every line into a tuple. Note: only 0,4 and 7th columns are selected.

val rdd_1 = rdd.filter(x=>x!= line1(0));//remove the first line

val rddtoDF = rdd_1.toDF("make","type","hp");//conver RDD to DF

rddtoDF.select("make","hp").show //execute any DF command of your choice.

