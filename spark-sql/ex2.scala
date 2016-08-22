val irisRDD = sc.textFile(datadir+"/iris.csv");
val headerRDD = irisRDD.first();
val irisRDD_body = irisRDD.filter(iris => iris!=headerRDD);
val irisRDDFeed = irisRDD_body.map(iris1 => iris1.split(",")).map(iris2=>(iris2(0),iris2(1),iris2(2),iris2(3),iris2(4)));
 val irisDF = irisRDDFeed.toDF("SLength","SWidth","PLength","PWidth","Species");
 irisDF.filter(irisDF("PWidth")>0.4).count; //102

//Method1: using RDD
 //group by using DF:
 irisDF.groupBy("Species").agg(avg("PWidth"));

//Method2: using temp table
 //register a temp table:
 irisDF.registerTempTable("iris");
sqlContext.sql("select Species,avg(PWidth) from iris group by Species").show;
