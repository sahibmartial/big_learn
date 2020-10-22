import org.apache.log4j._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.collection.mutable._
import sparkbigdata._
import kafkastreaming._
import org.apache.spark.rdd.RDD
object helloworld {
  val name: String="sahibmartial"
  //BasicConfigurator.configure()
  private var  trace_appli : Logger=LogManager.getLogger("Logger_Console")


  def main(args: Array[String]): Unit = {
    //creation dun df
  val session = Session_spark(true)
    val df_test= session.read
      .format("com.databricks.spark.csv")
      .option("delimiter",",")
      .option("header","true")
      .csv("/home/marubo/source_data/2010-12-06.csv")
    // df_test.show(7)

    val df_folder=session.read
      .format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("/home/marubo/source_data/mini/*")
    //df_folder.show(7)


    val df_gp2=session.read
      .format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("/home/marubo/source_data/mini/2010-12-06.csv","/home/marubo/source_data/mini/2011-12-08.csv")

   // df_gp2.show(10)

   // println("def_folder count : " +df_folder.count() + "df_gp2 count : " + df_gp2.count())

    //df_test.printSchema() //affiche les schema du df
   //operation de sur  colonne  dataframe ou creation de dataframe  a partir dun dataframe
    val df_test2=df_test.select(
      col("_c0").as("ID_Client").cast(IntegerType),
      col("StockCode").cast(IntegerType).as("CodeNumber"),
      col("Invoice".concat("No")).as("Id Commande").cast(IntegerType)
    )




    //manipulation avancee modifi col ou create col in dataframe au lieu de faire du select

    val df_test3= df_test.withColumn("InvoiceNo", col("InvoiceNo").cast(StringType))
      .withColumn("StockCode",col("StockCode").cast(IntegerType))
      .withColumn("valeurContaste",lit(50))
      .withColumnRenamed("_c0","Id_Client")
      .withColumn("Idcommande",concat_ws("|",col("InvoiceNo"),col("Id_Client")))
      .withColumn("totalcommnde",round(col("Quantity")* col("UnitPrice"),scale = 2)) //aarondi du resultat
      .withColumn("created_at",current_timestamp())
      .withColumn("reduction",when(col("totalcommnde")>15,lit(3)).otherwise(lit(0)))
      .withColumn("TotalComande_reduction",col("totalcommnde")-col("reduction"))
      .withColumn("reduction_case",when(col("totalcommnde")<15,lit(0))
        .otherwise(when(col("totalcommnde").between(15,20),lit(3))
          .otherwise(when(col("totalcommnde")>20,lit(4))))
      )
      //.show(15)
    val df_filter=df_test3.filter(col("reduction_case")===lit(0) && col("Country")
      .isin("United Kingdom","France","USA") )
      //.show(10)

    //jointure  sur colonne et aggreation
    val df_orders=session.read
      .format("com.databricks.spark.csv")
      .option("delimiter","\t")
      .option("header","true")
      //.option("inferSchema","true")
      .load("/home/marubo/source_data/orders.txt")

    val df_orderline=session.read
      .format("com.databricks.spark.csv")
      .option("delimiter","\t")
      .option("header","true")
      //.option("inferSchema","true")
      .load("/home/marubo/source_data/orderline.txt")

    val df_product=session.read
      .format("com.databricks.spark.csv")
      .option("delimiter","\t")
      .option("header","true")
      //.option("inferSchema","true")
      .load("/home/marubo/source_data/product.txt")

    //df_orders.show(5)
    //df_orderline.show(5)
  //  df_product.show(5)
    //df_orders.printSchema()
   // df_orderline.printSchema()
    //df_product.printSchema()


    val df_inner_join = df_orderline.join(df_orders,df_orders.col("orderid")===df_orderline.col("orderid"),"inner")
      .join(df_product,df_product.col("productid")===df_orderline.col("productid"),"inner")
      //.show(5)
     //df_inner_join.printSchema()
     val df_fullouter = df_orderline.join(df_orders,df_orders.col("orderid")===df_orderline.col("orderid"),"fullouter")
      // .show(5)

    //uninon jointure sur les lignes avoir meme structure mme de type pour les champ de colonne
    val df_fichier1=session.read
      .format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("/home/marubo/source_data/mini/2010-12-06.csv")

    val df_fichier2=session.read
      .format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("/home/marubo/source_data/mini/2011-01-20.csv")

    val df_fichier3=session.read
      .format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("/home/marubo/source_data/mini/2011-12-08.csv")


  val df_union=df_fichier1.union(df_fichier2.union(df_fichier3))

   // println("count Fichier3: "+df_fichier3.count()+" : "+ "count union file : "+df_union.count())

    //aggregat et fenetragee:

  }


  def manip_rdd(): Unit = {
    trace_appli.info("Debut test session spark")
   val ss=Session_spark(true)
    val session_s=Session_spark(true)
    val sc=ss.sparkContext
    sc.setLogLevel("OFF")
    val rdd_test : RDD[String]=sc.parallelize(List("Alain","Juvenal","Sahib"))
    rdd_test.foreach{
      l=>println(l)
    }
    val rdd_seq = sc.parallelize(Seq(("sahib","Math",15),("demco","Math",16),("marubo","Math",18)))
    /*rdd_seq.foreach{
      t=>println(t)
    }*/

    //rdd_seq.saveAsTextFile("/home/marubo/rdd.txt")
    println("RDD A PARTIR DE FOLDER")
    val rdd4 = sc.textFile("/home/marubo/RDD/*")
    //rdd4.foreach{r=>println(r)}
    val rdd_5=sc.parallelize(List("sahib mange une banane","la banane est utile","faire de la banane un aliment pour la sante"))
    rdd_5.foreach{ l=>println(l)}

    trace_appli.info(" Fin de la Session Spark ")
    hello()
    println("votre texte contient :"+compte_caracte("bonjour")+" characters")
    println()
    divisionentier(20,5)

    val rdd_map=rdd_5.map(x=>x.split(" "))
      println(" nbre elt de mon Map "+rdd_map.count())//affiche le nbr de ligne

    //transformons notre rdd-map
    val rdd_6=rdd_5.map(w=>(w,w.length,w.contains("banane")))//affiche chaque ligne , nre de cacart par ligne et verifie si ya le mote banane(3 colonnes)
   val rdd_7=rdd_6.map(x=>(x._1.toUpperCase(),x._2,x._3))


    //count each carct
    val rdd_8=rdd_7.map(x=>(x._1.split(" "),1))
    //count word with flatmap et pour chaque j'applique 1 comme count
    val rdd_fm=rdd_5.flatMap(x=>x.split(" ")).map(w=>(w,1))
    //

    val rdd_file=rdd4.flatMap(x=>x.split(" "))
  //  rdd_file.foreach(l=>println(l))
    //save in folder the result
   // rdd_file.repartition(1).saveAsTextFile("/home/marubo/countword.txt")

    //filter

    val rdd_filter=rdd_fm.filter(x=>x._1.contains("banane"))
    rdd_filter.foreach(l=>println(l))

    //use aggreagation pour compter le nbr de fois apparait un mot
    val rdd_reduce=rdd_fm.reduceByKey((x,y)=>x+y)
    rdd_reduce.foreach(l=>println(l))

    import session_s.implicits._
    val df : DataFrame =rdd_fm.toDF("texte","valeur")
    df.show(20)

  }

  //fisrt fonction
  def compte_caracte(carac: String) : Int={
    trace_appli.info("demarrage du tracage de la classe helloworld")

    trace_appli.info(s"tracage de log4j de mon aplli :$carac")
    if (carac.isEmpty){
      0
    }else{
      carac.trim.length
    }
  }

def hello()= {
  println("hello world mon 1er programme")
  println("Hello Ami Sahibmartial")
}

  def divisionentier(num:Int,deno:Int):Double= {
    if(deno!=null){
      val result=num/deno
      println(s"division possible et le resultat est : $result")
     return result
    }else{
     val result=0.0
      println(s"division par zero impossible: $deno")
      return result
    }
  }




  // getSparkStreamingContext(true,10)
  //getConskafka("localhost:9092","groupe_dsi","latest","","",10,Array("TutorialTopic"))
}
