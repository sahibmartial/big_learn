import org.apache.spark
import kafkastreaming._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{DataType, IntegerType, Metadata, StringType, StructField, StructType}
import sparkbigdata._
import org.apache.log4j.Logger
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}

object consommation_streaming {
  val bootStrapServers : String =""
  val consumerGroupId : String=""
  val consumerReadOrder : String=""
  val zookeeper : String=""
  val kerberosName : String =""
  val batchDuration :Int=15
  val topics : Array[String]=Array("")
  private var trace_conso_streaming :Logger =Logger.getLogger("Logger_Console")
  val checkpointpath:String="/hadoop/hgb/datalake"

  val exple_schema=StructType(Array(
    StructField("Zipcode",IntegerType,true),
    StructField("ZipCodeType",StringType,true),
    StructField("City",StringType),
    StructField("State",StringType,true)
  ))

  /**
   * fonction appele en cas de reprise
   * @param check_path : chemin dupath pour save le check point
   * @return
   */
  def check_point_SparkStreamoncontext(check_path:String):StreamingContext={
    val ssc2=getSparkStreamingContext(true,batchDuration)
    val kafkaStreams_cp=getConsumerSparkkafka(bootStrapServers,consumerGroupId,consumerReadOrder,zookeeper,kerberosName,topics,ssc2)

    ssc2.checkpoint(check_path)
    return ssc2
  }

  def main(args: Array[String]): Unit = {
    persistoffset_in_kafka()
  }

  /**
   * cette fonction gere la persistance des offsets dans un topic  de kafka
   */
  def persistoffset_in_kafka(): Unit ={
    val ssc=getSparkStreamingContext(true,batchDuration)

    //je consomme les datas ds le topic via ma fonction
    val kafkaStreams=getConsumerSparkkafka(bootStrapServers,consumerGroupId,consumerReadOrder,zookeeper,kerberosName,topics,ssc)//vue le contextstreeaming est geere par le cette fction il faut avoir le mm context
    //fisrt methode
    //j'ai la data je peux realiser les operations pour la visualisation
    //  val datastream=kafkaStreams.map(record => record.value())//cette methode ne gere pas l'offset donc les datas pvent traites au moins une fois

    //secpond methode gestion de l'offset methode recommandé
    kafkaStreams.foreachRDD{
      rddstream=>
        if(!rddstream.isEmpty()){
          //persistance des offsets pour gerer la semantique et coherence des datas
          val offset=rddstream.asInstanceOf[HasOffsetRanges].offsetRanges //persistances des offset en merory
          val datastream=rddstream.map(record=>record.value())//consommation des datas pas oblige de faire le parcours des partitions car les rdd et la partitions ds la consummerkafka(consumerrecord) sint lies

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
          val event_kafka2=data_frame.withColumn("tweet_events",from_json(col("tweet_events"),exple_schema))
            .select(col("tweet_events.*"))//recupee tte les colonnes
            .select(col("tweet_events.zipcode"))//select une colonne particuliere
          //appliquer les filtre avt le selct

          //semantique exactement  livraison et treatment exactement 1 fois, persiatance offset ds kafaka after treatment
          trace_conso_streaming.info("persistance des offsets ds log kafka en cours .....")
          kafkaStreams.asInstanceOf[CanCommitOffsets].commitAsync(offset)
          trace_conso_streaming.info("persistance des offsets ds log kafka terminer ac succes ")
        }
    }
    //demarage de la session spark
    ssc.start()
    ssc.awaitTermination()

  }

  /**
   * cette fonction gere la persistance des offsets ds un  checkpoint
   */
  def persistance_checkpoint(): Unit = {

    //cas ac check_point pour gere pannes
    val ssc_check_point = StreamingContext.getOrCreate(checkpointpath, () => check_point_SparkStreamoncontext(checkpointpath))
    val kafkaStreams_cp = getConsumerSparkkafka(bootStrapServers, consumerGroupId, consumerReadOrder, zookeeper, kerberosName, topics, ssc_check_point)
    //save checkppoint each 15
    kafkaStreams_cp.checkpoint(Seconds(15))


    //val ssc=getSparkStreamingContext(true,batchDuration)
    //je consomme les datas ds le topic via ma fonction
    //  val kafkaStreams=getConsumerSparkkafka(bootStrapServers,consumerGroupId,consumerReadOrder,zookeeper,kerberosName,topics,ssc)//vue le contextstreeaming est geere par le cette fction il faut avoir le mm context
    //fisrt methode
    //j'ai la data je peux realiser les operations pour la visualisation
    //  val datastream=kafkaStreams.map(record => record.value())//cette methode ne gere pas l'offset donc les datas pvent traites au moins une fois

    //secpond methode gestion de l'offset methode recommandé
    kafkaStreams_cp.foreachRDD {
      rddstream =>
        if (!rddstream.isEmpty()) {
          //persistance des offsets pour gerer la semantique et coherence des datas
          val offset = rddstream.asInstanceOf[HasOffsetRanges].offsetRanges //persistances des offset en merory
          val datastream = rddstream.map(record => record.value()) //consommation des datas pas oblige de faire le parcours des partitions car les rdd et la partitions ds la consummerkafka(consumerrecord) sint lies

          //ici je peux faire un commit pour dire j'i recu semantique livraison  au moins une fois
          //   kafkaStreams.asInstanceOf[CanCommitOffsets].commitAsync(offset)

          //Convertion en Dataframe des rdd pour treatmant avt de commit

          //creation une new session spark ac le sparkcontext utilise par les rdd du kafkaconsumer
          val ss = SparkSession.builder().config(rddstream.sparkContext.getConf).enableHiveSupport().getOrCreate()
          import ss.implicits._
          //create Data_frame tte les 15 secondes
          val data_frame = datastream.toDF("tweet_events") //obtention du dataframe a ceta stade on a pas connaissance de la structure de de notre datastream
          //on specifie la colonne
          //creation vue temporaire
          data_frame.createOrReplaceGlobalTempView("kafka_events")
          //executionn requete sql sur notre vue
          val event_kafka = ss.sql("select * from kafka_events") //data sous forme json
          event_kafka.show()

          //creation d'une structure pour manipuler au mieux les dataframes en amony preparer le schema pour 1 best treatment
          val event_kafka2 = data_frame.withColumn("tweet_events", from_json(col("tweet_events"), exple_schema))
            .select(col("tweet_events.*")) //recupee tte les colonnes
            .select(col("tweet_events.zipcode")) //select une colonne particuliere
          //appliquer les filtre avt le selct

          //semantique exactement  livraison et treatment exactement 1 fois, persiatance offset ds kafaka after treatment
          /* trace_conso_streaming.info("persistance des offsets ds log kafka en cours .....")
           kafkaStreams.asInstanceOf[CanCommitOffsets].commitAsync(offset)
           trace_conso_streaming.info("persistance des offsets ds log kafka terminer ac succes ")*/
        }
    }
    //demarage de la session spark
    // ssc.start()
    // ssc.awaitTermination()
    //cas de check_point
    ssc_check_point.start()
    ssc_check_point.awaitTermination()
  }



}
