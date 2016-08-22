val datadir = "/Users/vinodm1986/Workspace/spark/data/";
val irisRDD = sc.textFile(datadir+"iris.csv");
irisRDD.cache();
irisRDD.count
val versicolor = irisRDD.filter(x=>x.split(",")(4).equals("versicolor")).count;


val irisRDDWO = irisRDD.filter(sepal => sepal!=header);
def map(sepal:String):(Float,Int) = {
    val length = sepal.split(",")(0);
    (length.toFloat,1);
};
def reduce(sepal1:(Float,Int),sepal2:(Float,Int)):(Float,Int) = {
    (sepal1._1+sepal2._1,sepal1._2+sepal2._2);
};
val ave = irisRDDWO.map(map).reduce(reduce);
ave._1/ave._2


val average= sc.broadcast(5.84);
val count= sc.accumulator(0);


val aveCount = irisRDDWO.map((iris)=>{
    if(iris.split(",")(0).toFloat>average.value)count+=1;
}).count;

