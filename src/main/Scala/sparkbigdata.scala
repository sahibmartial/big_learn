import org.apache.spark.sql._
import  org.apache.spark.streaming._

object sparkbigdata {
var ss : SparkSession = null

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


}
