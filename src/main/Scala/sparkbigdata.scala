import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.streaming._

object sparkbigdata {
var ss : SparkSession = null
  var spConf   : SparkSession = null

  /**
   *
   * @param Env
   * @return
   */
  def Session_spark ( Env : Boolean =true) : SparkSession = {
    if (Env == true){
      System.setProperty("hadoop.home.dir", "")
      ss = SparkSession.builder()
        .master("local[*]")
        .config("spark.sql.crossJoin.enabled", "true")
        .enableHiveSupport()
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
   return ss
  }

  /**
   * initialisation du context spark streaming
   * @param env : environnement de deploiement de notre spark context true local sinon environnement réel
   * @param duree_batch : dure micro_batch
   * @return : renvoi une instance de park streaming
   */
  def getSparkStreamingContext(env :Boolean =true, duree_batch : Int) : StreamingContext ={
    trace_log.info("initialisation context sparkstreaming")
    if(env){
      spConf = new SparkConf().setMaster("LocalHost[*]")
        .setAppName("Mon application streaming")

    }else{
      spConf = new SparkConf().setAppName("Mon application streaming")
    }
    trace_log.info("La durée du microbatch est : $duree_batch secondes")
  val ssc : StreamingContext = new StreamingContext(spConf,Seconds(duree_batch))
    return ssc
  }


}
