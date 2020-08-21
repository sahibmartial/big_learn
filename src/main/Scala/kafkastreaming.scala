
import org.apache.spark.streaming.kafka010
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.serialization._
import org.apache.kafka.common
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.{Subscribe, _}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.KafkaUtils._
import sparkbigdata._
object kafkastreaming {

 var   kafkaparam : Map[String,Object]=Map(null,null)

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
  def getConskafka(kafkaBootStrapServers : String, KafkaConsumerGroupId : String, KafkaConsumerReadOrder : String,
                   KafkaZookeeper : String, KerberosName : String,batchDuration :Int,KafkaTopics : Array[Srting]) : InputDStream[ConsumerRecord[String,String]] = {

    val ssc = getSparkStreamingContext(true,batchDuration)
    kafkaparam = getKafkaSparkConsumerParams(kafkaBootStrapServers,KafkaConsumerGroupId,
      KafkaConsumerReadOrder ,KafkaZookeeper,KerberosName)
    val consokafka : InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream[String,String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](KafkaTopics,kafkaparam)//recupere trois infos de kafaka topic , partition et offset

    )

    return consokafka
  }



//Definition en dur de notre consummerkafka
/*  val ssc = getSparkStreamingContext(true,40)
   kafkaparam = getKafkaSparkConsumerParams("localHost:9092","groupe_DSI","latest","","")
  val consokafka : InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream[String,String](
    ssc,
    PreferConsistent,
    Subscribe[String, String]("Topic1",kafkaparam)//recupere trois infos de kafaka topic , partition et offset

  )*/

}


