import org.apache.log4j._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable._
import sparkbigdata._
import kafkastreaming._
object helloworld {
  val name: String="sahibmartial"
  //BasicConfigurator.configure()
  private var  trace_appli : Logger=LogManager.getLogger("Logger_Console")
  def main(args: Array[String]): Unit = {
    println("hello world mon 1er programme")

    println("votre texte contient :"+compte_caracte("bonjour")+" characters")
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
  println("hello Ami Sahibmartial")

 // getSparkStreamingContext(true,10)
  //getConskafka("localhost:9092","groupe_dsi","latest","","",10,Array("TutorialTopic"))
}
