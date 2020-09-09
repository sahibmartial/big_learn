
import java.time.Duration

import org.apache.spark.streaming.kafka010._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization._
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.protocol
import org.apache.kafka.common.serialization._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.{Subscribe, _}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.KafkaUtils._
import org.apache.kafka.common.serialization.StringDeserializer
import sparkbigdata._
import org.apache.log4j._
import java.util.Properties
import java.util.Collections
import scala.collection.JavaConverters._
import org.apache.kafka.common.internals.Topic

object kafkastreaming {



  //Creation d'un consumer et producer kafka via SparK

 var   kafkaparam : Map[String,Object]=Map(null,null)
  private var trace_conso : Logger=Logger.getLogger("Logger_Console")

  /**
   *
   * @param kafkaBootStrapServers : @ ip et port des agents kafka
   * @param KafkaConsumerGroupId : id du consumer group
   * @param KafkaConsumerReadOrder : ordre de lecture des logs
   * @param KafkaZookeeper : @ ip et port de  de zookeeper
   * @param KerberosName : service kerberos pour la securite
   * @return : retourne une table clé valeur des parametres de connexions
   */
  def getKafkaSparkConsumerParams ( kafkaBootStrapServers : String, KafkaConsumerGroupId : String, KafkaConsumerReadOrder : String,
                                    KafkaZookeeper : String, KerberosName : String) : Map[String, Object] = {
    kafkaparam = Map(
      "bootstrap.servers" -> kafkaBootStrapServers,
      "groupe.id"  -> KafkaConsumerGroupId,
      "zookeeper.hosts" -> KafkaZookeeper,
      "auto.offset.reset" -> KafkaConsumerReadOrder,
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "sasl.kerberos.service.name" -> KerberosName,
      "security.protocol" -> SecurityProtocol.PLAINTEXT
    )

    return kafkaparam

  }

  /**
   *
   * @param kafkaBootStrapServers : @ et port des agents kafka
   * @param KafkaConsumerGroupId : Id du consumer group
   * @param KafkaConsumerReadOrder:ordre de lecture des logs
   * @param KafkaZookeeper:  @ip et port de zookeeper
   * @param KerberosName: nom du service kerbreos
   * @param batchDuration: duree du batch
   * @param KafkaTopics:nom du topic
   * @return reourne une table clé valeur du topic
   */

  def getConsumerSparkkafka(kafkaBootStrapServers : String, KafkaConsumerGroupId : String, KafkaConsumerReadOrder : String,
                   KafkaZookeeper : String, KerberosName : String,batchDuration :Int,KafkaTopics : Array[String]) : InputDStream[ConsumerRecord[String,String]] = {
    trace_conso.info("initialisation  ConsoKfaka Streaming")
    val ssc = getSparkStreamingContext(true,batchDuration)
    kafkaparam = getKafkaSparkConsumerParams(kafkaBootStrapServers,KafkaConsumerGroupId,
      KafkaConsumerReadOrder ,KafkaZookeeper,KerberosName)
    val consokafka : InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream[String,String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](KafkaTopics,kafkaparam)//recupere trois infos de kafaka topic , partition et offset

    )
    trace_conso.info("Ends Conso Streaming")

    return consokafka
  }

  //Creation un consumer et producer via API KAFKA

  /**
   *
   * @param kafkabootsrapservers
   * @return
   */
 def getkafkaproducerparam(kafkabootsrapservers :String ): Properties = {
   val props : Properties = new Properties()
   props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
   props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
   props.put("acks","all")
   props.put("bootstrap.servers",kafkabootsrapservers)
   props.put("security.protocol","SASL_PLAINTEXT")
   return props
 }

  /**
   *
   * @param kafkabootsrapservers
   * @param topic_name
   * @param message
   * @return
   */
  def getproducerkafka(kafkabootsrapservers :String, topic_name:String,message: String) : KafkaProducer[String,String]={
    trace_conso.info(s"initialisation et connexion  au producer kafka  ${kafkabootsrapservers}")
  lazy  val producer_kafka= new KafkaProducer[String,String](getkafkaproducerparam(kafkabootsrapservers))
    try{
      trace_conso.info(s"message a pulbier dans le topic  par le producer kafka ${topic_name}, ${message}")
      val record_publish= new ProducerRecord[String,String](topic_name,message)
      trace_conso.info(s" publication du message via notre producer ${record_publish}")
      producer_kafka.send(record_publish)
    }catch {
      case ex: Exception =>
        trace_conso.error(s"erreur dans la publication de kafka ${ex.printStackTrace()}")
        trace_conso.error(s"la liste des param de connexion au producer sont${getkafkaproducerparam(kafkabootsrapservers)}")
    }finally {
      //producer_kafka.close() //via on consomme massive distribue le fermer ds la source streaming
    }
      return producer_kafka
  }

  /**
   *
   * @param kafkabootsrapservers
   * @param KafkaConsumerGroupId
   * @return
   */
  def getkafkaconsumerparams(kafkabootsrapservers:String,KafkaConsumerGroupId:String) : Properties={
    val props : Properties= new Properties()
    props.put("key.serializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.serializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.put("groupe.id",KafkaConsumerGroupId)
    props.put("bootstrap.servers",kafkabootsrapservers)
    props.put("enable.auto.commit" ,"false")
    props.put("auto.offset.reset", "latest")
    props.put("security.protocol","SASL_PLAINTEXT")
    return props
  }

  /**
   *
   * @param kafkabootsrapservers
   * @param KafkaConsumerGroupId
   * @param topic_list
   * @return
   */
  def getclientkafkaConso(kafkabootsrapservers:String,KafkaConsumerGroupId:String,topic_list: String):KafkaConsumer[String,String]={
   trace_conso.info("init du Consumer kafka ")
    val consumer= new KafkaConsumer[String,String](getkafkaconsumerparams(kafkabootsrapservers,KafkaConsumerGroupId))
    consumer.subscribe(Collections.singleton(topic_list))
    try{
      while(true){
        val messages : ConsumerRecords[String,String]=consumer.poll(Duration.ofSeconds(30))
        //Fisrt methode
        if(!messages.isEmpty){
          trace_conso.info("Message collectes dans la fenetre "+messages.count())
          for( message <- messages.asScala){
            println("Topic "+message.topic()+
              "Key "+message.key()+
              "Value "+message.value()+
              "Offset "+message.offset()+
              "Partition "+message.partition()
            )

          }
          try{
            consumer.commitAsync()
          }catch {
            case ex: CommitFailedException =>
              trace_conso.error("erreur lors du comit de l'offset, jeton de reconnaisance non  recu")
          }

        }
        //second method pour recuperer les messages iteration des messages
        /*val msgIterator=messages.iterator()
        while(msgIterator.hasNext==true){
          val msg=msgIterator.next()
          println(msg.key()+msg.partition()+msg.topic()+msg.value())
        }*/

      }
    }catch {
      case except:Exception =>
        trace_conso.error(s"erreur dans le consumer : ${except.printStackTrace()}")

    }finally {
      consumer.close()
    }

    return consumer
  }


  //creation  une source de données streaming(API Streaming) pour alimentyer  le producer  developper afin de push ds kafka  exple twitter
  //client API HOSEBIRD(HBC)


//Definition en dur de notre consummerkafka
/*  val ssc = getSparkStreamingContext(true,40)
   kafkaparam = getKafkaSparkConsumerParams("localHost:9092","groupe_DSI","latest","","")
  val consokafka : InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream[String,String](
    ssc,
    PreferConsistent,
    Subscribe[String, String]("Topic1",kafkaparam)//recupere trois infos de kafaka topic , partition et offset

  )*/

}


