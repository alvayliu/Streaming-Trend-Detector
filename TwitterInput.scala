package main
import java.text.SimpleDateFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

/**
  * Producer class that connects to twitter and gets twitter stream,
  * starts a kafka producer and produce the twitter stream to a kafka broker
  * on port 9092.
  */
object TwitterInput extends App {

    //Connect to twitter and get twitter stream
    val CONSUMER_API_KEY = "API_KEY"
    val CONSUMER_API_SECRET = "API_SECRET"
    val ACCESS_TOKEN = "ACCESS_TOKEN"
    val ACCESS_TOKEN_SECRET = "TOKEN_SECRET"

    //Configuration of kafka producer and twitter specifics
    val config = new SparkConf().setAppName("twitter-stream-producer").setMaster("local[*]")
    val sc = new SparkContext(config)
    sc.setLogLevel("WARN")
    sc.setCheckpointDir("./checkpoints")
    val ssc = new StreamingContext(sc, Seconds(1))
    val cb = new ConfigurationBuilder
    cb.setDebugEnabled(true).setOAuthConsumerKey(CONSUMER_API_KEY)
      .setOAuthConsumerSecret(CONSUMER_API_SECRET)
      .setOAuthAccessToken(ACCESS_TOKEN)
      .setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET)
    val auth = new OAuthAuthorization(cb.build)
    val filters = Seq("Spotify")      //chosen stock or term to track

    // Connect and get stream of english tweets
    val stream = TwitterUtils.createStream(ssc, Some(auth), filters)
    val englishTweets = stream.filter(_.getLang == "en")
    val tweetRDD = englishTweets.map(tweet => (tweet.getCreatedAt, tweet.getText))
    // Convert date to wanted format
    val outputFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm")
    val tweetDate = tweetRDD.map(tweet => (outputFormat.format(tweet._1), tweet._2))


    //set up kafka producer
    val topic = "trends"
    val brokers = "localhost:9092"

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "TwitterProducer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)


    //push tweet to kafka broker
    while (true){
      tweetDate.foreachRDD { rdd =>
        val records = rdd.map(tweet => new ProducerRecord[String, String](topic, tweet._1, tweet._2))
        records.foreach { record =>
          producer.send(record)
        }
      }
      ssc.start()
      ssc.awaitTermination()
     }

    producer.close()
}
