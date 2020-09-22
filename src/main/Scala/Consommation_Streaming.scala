
import org.apache.spark
import org.apache.spark.sql.functions.{col, from_json}
import kafkastreaming._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}

class Consommation_Streaming {


  val schema = StructType(Array(
    StructField("ville",StringType,true)
  ))
  def main(args: Array[String]): Unit = {


//cas ac check_point pour gere pannes
/* val ssc_check_point=StreamingContext.getOrCreate(checkpointpath,()=>check_point_SparkStreamoncontext(checkpointpath))
 val kafkaStreams_cp=getConsumerSparkkafka(bootStrapServers,consumerGroupId,consumerReadOrder,zookeeper,kerberosName,topics,ssc_check_point)
   //save checkppoint each 15
    kafkaStreams_cp.checkpoint(Seconds(15))
*/
// val ssc=getSparkStreamingContext(true,batchDuration)

//je consomme les datas ds le topic via ma fonction
// val kafkaStreams=getConsumerSparkkafka(bootStrapServers,consumerGroupId,consumerReadOrder,zookeeper,kerberosName,topics,ssc)//vue le contextstreeaming est geere par le cette fction il faut avoir le mm context
//fisrt methode
//j'ai la data je peux realiser les operations pour la visualisation
//  val datastream=kafkaStreams.map(record => record.value())//cette methode ne gere pas l'offset donc les datas pvent traites au moins une fois

//secpond methode gestion de l'offset methode recommandé

/* kafkaStreams.foreachRDD{
   rddstream=>
   if(!rddstream.isEmpty()){
     //persistance des offsets pour gerer la semantique et coherence des datas
      val offset=rddstream.asInstanceOf[HasOffsetRanges].offsetRanges //persistances des offset en merory
     val datastream=rddstream.map(record=>record.value())//consommation des datas pas oblige de faire le parcours des partitions car les rdd et la partitions ds la consummerkafka(consumerrecord) sont lies

     //ici je peux faire un commit pour dire j'i recu semantique livraison  au moins une fois
  //   kafkaStreams.asInstanceOf[CanCommitOffsets].commitAsync(offset)

     //Convertion en Dataframe des rdd pour treatmant avt de commit

     //creation une new session spark ac le sparkcontext utilise par les rdd du kafkaconsumer
     val ss =SparkSession.builder().config(rddstream.sparkContext.getConf).enableHiveSupport().getOrCreate()
     import ss.implicits._
     //create Data_frame tte les 15 secondes
     val data_frame=datastream.toDF("tweet_events")//obtention du dataframe a ceta stade on a pas connaissance de la structure de de notre datastream
     //on specifie la colonne
     //creation vue temporaire
     data_frame.createOrReplaceGlobalTempView("kafka_events")
     //executionn requete sql sur notre vue
     val event_kafka=ss.sql("select * from kafka_events")//data sous forme json
     event_kafka.show()

     //creation d'une structure pour manipuler au mieux les dataframes en amony preparer le schema pour 1 best treatment
     val event_kafka2=data_frame.withColumn("tweet_events",from_json(col("tweet_events"),schema))
       .select(col("tweet_events.*"))//recupee tte les colonnes
       .select(col("tweet_events.zipcode"))//select une colonne particuliere
     //appliquer les filtre avt le selct

     //semantique exactement  livraison et treatment exactement 1 fois, persiatance offset ds kafaka after treatment
     trace_conso_streaming.info("persistance des offsets ds log kafka en cours .....")
     kafkaStreams.asInstanceOf[CanCommitOffsets].commitAsync(offset)
     trace_conso_streaming.info("persistance des offsets ds log kafka terminer ac succes ")
   }
 }*/


//demarage de la session spark
// ssc.start()
// ssc.awaitTermination()
//cas de check_point
/*  ssc_check_point.start()
  ssc_check_point.awaitTermination()*/

}


}



