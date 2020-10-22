import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.log4j._
import org.apache.spark.rdd.RDD

object sparkbigdata {
var ss : SparkSession = null
  var spConf   : SparkConf= null
  private var trace_spark :Logger =Logger.getLogger("Logger_Console")

  def main(args: Array[String]): Unit = {
    println("hello spark big data")
  }

  /**
   *
   * @param Env
   * @return
   */
  def Session_spark ( env : Boolean =true) : SparkSession = {
    try{
      if (env == true){
        System.setProperty("hadoop.home.dir", "/usr/local/hadoop")
        ss = SparkSession.builder()
          .master("local[*]")
          //.config("spark.sql.crossJoin.enabled", "true")
          //   .enableHiveSupport() //doit etree installer sinon error laors du lancement
          .getOrCreate()

      }else{
        ss  = SparkSession.builder()
          .appName("application Name")
          .master("local[*]")
          .config("spark.serialiszer", "org.apache.serializer.kryoserializer")
          .config("spark.sql.crossJoin.enabled", "true")
          .enableHiveSupport()
          .getOrCreate()
      }
    }catch {
      case ex: Exception =>trace_spark.error("error lors init session spark"+ ex.printStackTrace())
    }
   return ss
  }

  /**
   * initialisation du context spark streaming
   * @param env : environnement de deploiement de notre spark context true local sinon environnement réel
   * @param duree_batch : dure micro_batch
   * @return : renvoi une instance de park streaming
   */

  def getSparkStreamingContext(env :Boolean =true, duree_batch : Int) : StreamingContext ={
    trace_spark.info("initialisation context sparkstreaming")
      if(env){
        spConf = new SparkConf().setMaster("LocalHost[*]")
          .setAppName("Mon application streaming")
      }else{
        spConf = new SparkConf().setAppName("Mon application streaming")
      }
    trace_spark.info(s"La durée du microbatch est : $duree_batch secondes")
    val ssc : StreamingContext = new StreamingContext(spConf,Seconds(duree_batch))
    return ssc
  }


}
