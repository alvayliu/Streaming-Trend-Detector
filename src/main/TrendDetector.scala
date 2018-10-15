package main

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
//import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

object TrendDetector {
  def main(args: Array[String]) {
    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS trend_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS trend_space.trends (time text PRIMARY KEY, total float, pos float, neg float, other float)")

    // make a connection to Kafka and read (key, value) pairs from it

    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000")
    val topics = Set("trends")
    val sparkConf = new SparkConf().setAppName("TrendDetector").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint("checkpoints")

    // Twitter input stream
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topics)

    val CONSUMER_API_KEY = "PNDlIzHgUUIauFCdVolig8A4F"
    val CONSUMER_API_SECRET = "XL5SP49lDkTRWlY6OSdZzSVjdY4IE5mOAtTFzbfPcWQiIGSSJ2"
    val ACCESS_TOKEN = "938324905346641920-gaTcdB5iR0NlBcHCZFicknYXHRGZ9MD"
    val ACCESS_TOKEN_SECRET = "cbSjh6rvJC0C4FcDFbrUfqECGDfyLqjokIXrbOSL4cTjL"

    // Twitter App API Credentials - underlying twitter4 Library
    System.setProperty("twitter4j.oauth.consumerKey", CONSUMER_API_KEY)
    System.setProperty("twitter4j.oauth.consumerSecret", CONSUMER_API_SECRET)
    System.setProperty("twitter4j.oauth.accessToken", ACCESS_TOKEN)
    System.setProperty("twitter4j.oauth.accessTokenSecret", ACCESS_TOKEN_SECRET)


    val filters = Seq("iphone","samsung galaxy")
    val twitterStream = TwitterUtils.createStream(ssc,None,filters)
    val englishTweets = twitterStream.filter(_.getLang == "en")
    englishTweets.map(_.getText).print()

    // Function for sentiment analysis


    // Write to cassandra
  }


}
