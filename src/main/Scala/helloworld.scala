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

    hello()
    println("votre texte contient :"+compte_caracte("bonjour")+" characters")
    println()
    divisionentier(20,5)
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
