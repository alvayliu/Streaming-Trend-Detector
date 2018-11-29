package main
import org.apache.spark.SparkContext
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.StateSpec
import com.datastax.spark.connector._
import com.datastax.driver.core.Cluster
import com.datastax.spark.connector.streaming._

/**
  * Consumer class that reads data from Kafka and performs sentiment analysis,
  * counts the sentiments and updates the counts in the Cassandra table.
  */
object TrendDetector {
  def main(args: Array[String]) {
    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS trend_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS trend_space.trends (datetime text PRIMARY KEY, pos float, neg float, neu float)")

    // make a connection to Kafka and read (key, value) pairs from it
    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000")

    val config = new SparkConf().setAppName("twitter-stream-sentiment").setMaster("local[*]")
    val sc = new SparkContext(config)
    sc.setLogLevel("WARN")
    sc.setCheckpointDir("./checkpoints")
    val ssc = new StreamingContext(sc, Seconds(5))
    val topics = Set("trends")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topics)

    //sentiment analysis of kafka stream
    val tweetRDD = messages.map { tweet =>
      val sentiment = SentimentDetector.getSentiment(tweet._2)
      (sentiment+";"+tweet._1, "")
    }

    //update total values
    val stateDstream = tweetRDD.mapWithState(StateSpec.function((key: String, value: Option[String], state: State[Long])=>{
      val sentiment = key.split(";")(0)
      val date = key.split(";")(1)
      val tot = state.getOption.getOrElse(0L) + 1
      val output = (date, sentiment, tot)
      state.update(tot)
      output
    }))

    //save to cassandra
    stateDstream.filter(x => x._2 == "POSITIVE").map(x => (x._1, x._3)).saveToCassandra("trend_space", "trends", SomeColumns("datetime", "pos"))
    stateDstream.filter(x => x._2 == "NEGATIVE").map(x => (x._1, x._3)).saveToCassandra("trend_space", "trends", SomeColumns("datetime", "neg"))
    stateDstream.filter(x => x._2 == "NEUTRAL").map(x => (x._1, x._3)).saveToCassandra("trend_space", "trends", SomeColumns("datetime", "neu"))

    ssc.start()
    ssc.awaitTermination()
  }
}
