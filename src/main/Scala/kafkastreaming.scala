import org.apache.spark.streaming.kafka010
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.serialization.StringDeserializer

object kafkastreaming {
 var  kafkaparam : Map[String,Object]=Map(null,null)

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
}
