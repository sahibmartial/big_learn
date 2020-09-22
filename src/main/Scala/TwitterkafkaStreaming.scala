import com.twitter.hbc.httpclient.auth._
import com.twitter.hbc.core.{Client, Constants}
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}
import java.util.Collections

import com.twitter.hbc.ClientBuilder

import scala.collection.JavaConverters._
import com.twitter.hbc.core.endpoint._
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import kafkastreaming._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql
import org.apache.spark.streaming.Minutes
import twitter4j._
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.{Configuration, ConfigurationBuilder}
import org.apache.spark.streaming.twitter.TwitterUtils
import sparkbigdata._
import java.sql.{Connection, DriverManager, ResultSet, SQLException}

import consommation_streaming.batchDuration
import org.apache.spark.streaming.kafka010.KafkaUtils

/**
 * Cette classe permet de creer un flux streaming a partir de twitter  que nous allons coupler a notre producer de kafka pour du streaming
 */
class TwitterkafkaStreaming {
 private var trace_twitter :Logger=Logger.getLogger("Logger_Console")

  //creation des paramatres pour se conecter a twiter
  private def twitterauthenconf(CONSUMER_KEY:String,CONSUMER_SECRET:String,ACCESS_TOKEN:String,TOKEN_SECRET:String) : ConfigurationBuilder = {
    val twitterconfig : ConfigurationBuilder = new ConfigurationBuilder()
    twitterconfig
      .setDebugEnabled(true)
      .setGZIPEnabled(true)
      .setJSONStoreEnabled(true)
      .setOAuthConsumerKey(CONSUMER_KEY)
      .setOAuthConsumerSecret(CONSUMER_SECRET)
      .setOAuthAccessToken(ACCESS_TOKEN)
      .setOAuthAccessTokenSecret(TOKEN_SECRET)
    return twitterconfig
  }

  /**
   * client hbc qui collecte les tweets contenant une liste de tags et les publie dans le client publish-suscribe du cluster kafka
   * @param CONSUMER_KEY :cle d'authent Oauth pour la consom
   * @param CONSUMER_SECRET : scert cle dde conso
   * @param ACCESS_TOKEN : acces token de l'authen
   * @param TOKEN_SECRET : token secret de l'authen
   * @param liste_hastags :liste des tags des txeeets que l'on souhaite collectes
   * @param kafkabootsrapservers : @ip des gaents kafka et port du cluster
   * @param topic :le(s) topc(s) ds le(s) stocke(s) les tweet(s)
   */
  def producertwitterKfakaHbc(CONSUMER_KEY:String,CONSUMER_SECRET:String,ACCESS_TOKEN:String,TOKEN_SECRET:String,
                               liste_hastags:String,kafkabootsrapservers:String,topic:String): Unit ={

    trace_twitter.info("Init de la queue  streaming twitter :")
    val queue :BlockingQueue[String] = new LinkedBlockingQueue[String](10000)
   //  queue.poll(10,TimeUnit.SECONDS) //reception temps reel des tweets

        trace_twitter.info("Init param authentification  de  pour Application twitter :")
        val auth: Authentication = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, TOKEN_SECRET)

        trace_twitter.info("Init du endpoint et du flitre( terme a tracker les hastag de twitter exple :#big_data)  dans  twitter :")
        val edp: StatusesFilterEndpoint = new StatusesFilterEndpoint()
        edp.trackTerms(Collections.singletonList(liste_hastags))
        //edp.trackTerms(List(liste_hastags).asJava) //collections list

        trace_twitter.info("Init des params du client HBC:")
        val parmam_hbc: ClientBuilder = new ClientBuilder()
          .hosts(Constants.STREAM_HOST)
          .authentication(auth)
          .gzipEnabled(true)
          .endpoint(edp)
          .processor(new StringDelimitedProcessor(queue))
        trace_twitter.info("Connexion  au client HBC et recuperation des tweets:")
        val client_hbc: Client = parmam_hbc.build()
        client_hbc.connect()//actions va declencher tout les ttapes d'en haut pour etablir la connexion du client hbc
        try {
          while (client_hbc.isDone) {
            val tweets: String = queue.poll(15, TimeUnit.SECONDS)
            try{
              trace_twitter.info("Appel du client publish-suscribe du system kafka pour persister ds les logs, apres  :")
              getproducerkafka(kafkabootsrapservers, topic, tweets)
              println("message " + tweets)
            }catch {
              case ex :Exception =>
                trace_twitter.error(s"erreur lors de la recuperation des tweets par notre producer kafka: ${getproducerkafka(kafkabootsrapservers, topic, tweets)}")
            }
          }
        }catch {
          case excep:InterruptedException =>
            trace_twitter.error(s"erreur de connexion a notre client HBC: ${excep.printStackTrace()}")
        }finally {
          getproducerkafka(kafkabootsrapservers, topic, "").close()//stop mon producer after use
          client_hbc.stop() //ferme  le client hbc

        }

      }

  /***
   *
   * @param CONSUMER_KEY
   * @param CONSUMER_SECRET
   * @param ACCESS_TOKEN
   * @param TOKEN_SECRET
   * @param liste_hastags
   * @param kafkabootsrapservers
   * @param topic
   * @param requete
   */

  def producertwitter4jKafka(CONSUMER_KEY:String,CONSUMER_SECRET:String,ACCESS_TOKEN:String,TOKEN_SECRET:String,
                              liste_hastags:String,kafkabootsrapservers:String,topic:String,requete:String): Unit ={
    val queue :BlockingQueue[Status]=new LinkedBlockingQueue[Status](10000) //creation de ma queue
/*
    val twitterconf : ConfigurationBuilder = new ConfigurationBuilder() //creation des paramatres pour me conecter a twiter
    twitterconf
       .setDebugEnabled(true)
       .setGZIPEnabled(true)
       .setJSONStoreEnabled(true)
       .setOAuthConsumerKey(CONSUMER_KEY)
       .setOAuthConsumerSecret(CONSUMER_SECRET)
       .setOAuthAccessToken(ACCESS_TOKEN)
       .setOAuthAccessTokenSecret(TOKEN_SECRET)

*/
      val twitterstream  = new TwitterStreamFactory(twitterauthenconf(CONSUMER_KEY,CONSUMER_SECRET,ACCESS_TOKEN,TOKEN_SECRET).build()).getInstance()//creation de mon client stream et connxeion a twitter

    //setp creation du listener pour recover les msgses
    val listener= new StatusListener {
      override def onStatus(status: Status): Unit ={
        trace_twitter.info(s"evenement new tew tweet : ${status.getText}")
        queue.put(status)
        //fisrt methode apple du producer kafkaproducer pour save in cluster ma qeue doit etre de type String et mettre le try and catch
     //   getproducerkafka(kafkabootsrapservers, topic,status.getText) //push ds le cluster topic: topic_name
      }

      override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit ={}
      override def onTrackLimitationNotice(i: Int): Unit = {}
      override def onScrubGeo(l: Long, l1: Long): Unit ={}
      override def onStallWarning(stallWarning: StallWarning): Unit ={}

      override def onException(e: Exception): Unit ={
        trace_twitter.info(s"Error dans twitter : ${e.printStackTrace()}")
      }

    }
    twitterstream.addListener(listener)
    //twitterstream.sample()//declenche et capture tous les tweets sans filtrer en effectuant ts les etapes ce qui est en haut
   val query: FilterQuery= new FilterQuery().track(requete) //ici requete ce sont nos hastag a rechercher à filter
    twitterstream.filter(query)// je recupere les msg du filtre du hastag specifie

    //second method la queue est de type Status mettre le try and catch
    try{
      while(true){
        val tweets: Status = queue.poll(15, TimeUnit.SECONDS)

        getproducerkafka(kafkabootsrapservers, topic,tweets.getText)
      }

    }catch {
      case e:InterruptedException=>
        trace_twitter.info(s"Error ds le client publish-suscribe : ${getproducerkafka(kafkabootsrapservers, topic,"")}")
    }finally {
      getproducerkafka(kafkabootsrapservers, "","").close()
      twitterstream.shutdown()
    }

  }

  //creation du client streaming spark
  /**
   * client spark streaming publier ds le producer kafka
   * @param CONSUMER_KEY
   * @param CONSUMER_SECRET
   * @param ACCESS_TOKEN
   * @param TOKEN_SECRET :param authenificcation
   * @param kafkabootsrapservers : @ip et port agents kafka
   * @param topic : nom du topic ds lekel le publie les msg ds le ssyteme  de messagerie
   * @param filtre : on specifie le tague research exple #big_data
   */
  def clientsparkstreamingkfakatwitter(CONSUMER_KEY:String,CONSUMER_SECRET:String,ACCESS_TOKEN:String,TOKEN_SECRET:String,
                                       kafkabootsrapservers:String,topic:String,filtre:Array[String]): Unit ={
    //creation authentification
    val authen= new OAuthAuthorization(twitterauthenconf(CONSUMER_KEY,CONSUMER_SECRET,ACCESS_TOKEN,TOKEN_SECRET).build())

    //creation du client streaming
    val twittersparkstream = TwitterUtils.createStream(getSparkStreamingContext(true,15),Some(authen),filtre) //Some pour dire que aunthen est optionnel
     //creation des filtres ensmble de RDD a manipuler avt de les persister don ce sont ces variables que je persiste ds les logs kafka

    val tweetmsg=twittersparkstream.flatMap(status=>status.getText())//on part du fait on precise le filtre #big_data
    val tweetcomplet=twittersparkstream.flatMap(Status=>Status.getText() ++ Status.getContributors() ++ Status.getLang())
    val tweetFr=twittersparkstream.filter(status=>status.getLang()=="fr")
    //on utilise filter qd on a un boolen ds les rdd
    //split le text du symbol hastag
    val hastag=twittersparkstream.flatMap(status=>status.getText().split(" ").filter(status=>status.startsWith("#")))

    //manipulation des tweets Fr
    val hastagfr=tweetFr.flatMap(status=>status.getText().split(" ").filter(status=>status.startsWith("#")))
     //recuperation ds counts  fr after 5 mn
    val hastagfrcount=hastagfr.window(Minutes(5))
    //recuper les tweets taguer big_data A revoir
    val tweetfr_bigdata=tweetFr.flatMap(status =>status.getText().split(" ").filter(status=>status.startsWith("#big_data")))// separe par des espaces puis rechercher ceux qui commence par bigdata

    //persister ds un fichiier
    tweetmsg.saveAsTextFiles("tweet","json")

    //Publication ds le producer kafkaproducer psersister ds system distant kafka
    //fisrt  methode

    tweetmsg.foreachRDD{
      tweetrdd =>
        try{
          if(!tweetrdd.isEmpty()){
            tweetrdd.foreachPartition{
              tweetpartition =>tweetpartition.foreach{ tweet =>
                getproducerkafka(kafkabootsrapservers,topic,tweet.toString())
              }
            }
          }
        }catch {
          case e:Exception => trace_twitter.error("Error ds la publication ds le producer kafka"+e.printStackTrace())
        }finally {
          getproducerkafka(kafkabootsrapservers,"","").close()
        }
    }

   // second methode recommande pas Spark
    tweetmsg.foreachRDD{
      (tweetrdd,temps)=>
          if(!tweetrdd.isEmpty()){
            tweetrdd.foreachPartition{
              tweetpartition =>
                val producer_kafka= new KafkaProducer[String,String](getkafkasparkproducerparam(kafkabootsrapservers))
                tweetpartition.foreach{
                  tweet=>
                    val record_publish= new ProducerRecord[String,String](topic,tweet.toString())
                    try{
                      producer_kafka.send(record_publish)
                    }catch {
                      case e:Exception=>trace_twitter.error("Error ds la publication ds le producer kafka"+e.printStackTrace())
                    }finally {
                      producer_kafka.close()
                    }
                }

            }
          }

    }


  //Demarrage de la session spark
    getSparkStreamingContext(true,15).start()
    getSparkStreamingContext(true,15).awaitTermination()//attend que tout le microbacth soittermine pour lancer un autte
   //pour stopper sinon boucle a infini
   //getSparkStreamingContext(true,15).stop()

  }


  /**
   *
   * se  connecter la bd , execute une requete sql et passser le resultat a mon producer qui ecoute en permanence
   */

  def producer_stream_db(kafkabootsrapservers:String,topic:String,batchDuration:Int)={
    val ssc=getSparkStreamingContext(true,batchDuration)
    val infos  = connectbd()

      if(!infos.isEmpty){
        getproducerkafka(kafkabootsrapservers,topic,infos)
        getproducerkafka(kafkabootsrapservers,"","").close()
      }else{
        println("info vide")
      }

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * fonction JDBC pour se connecter a une bd via ApI  de java pour recuperer les infos de la bd
   */
  def connectbd() :String ={
    // connect to the database named "mysql" on the localhost
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost/mysql"
    val username = "root"
    val password = "root"
    var connection:Connection = null
    val resultSet :String =""
    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)

      /*val   connect =
        DriverManager.getConnection("jdbc:mysql://localhost/test?" +
          "user=minty&password=greatsqldb")*/

      // create the statement, and run the select query
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT host, user FROM user")//retourne un json
      while (resultSet.next()) {
        val host = resultSet.getString("host")
        val user = resultSet.getString("user")
        println("host, user = " + host + ", " + user)
      }
    }catch {
      case ex:Exception=>ex.printStackTrace()
    }finally {
      connection.close()
    }
    return resultSet
  }


}
