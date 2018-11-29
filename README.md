# Streaming Trend Detector

This repository contains the code to a real-time trend tracker of an user-specified keyword written by me and 
[Michelle Jagelid](https://github.com/jagelid) as a part of the course ID2221 Data-intensive Computing at KTH. 
For a short video demonstration of the results, see [here](https://youtu.be/hk81pQYi52w).
 
The program has four stages. All stages except the visualization are implemented in Scala.
The visualization is implemented in Python.

### 1. Input Stream and Kafka
Input data for the Kafka producer is a stream from Twitter's Streaming API filtered on a certain keyword such as ”Spotify” or ”Verizon”. 
We restricted the input stream to contain only English tweets. The data is processed so that only the timestamp of the tweet and the text
content are pushed as a key-value pair to the Kafka broker. In this way, the program can be easily extended by including more Kafka 
producers that transform additional data streams to a common format (date, text) before pushing it to the broker. The data is transformed 
to a common format in order for Kafka clients to handle all data entries in the same way regardless of their original input format from the
source.

Example of tweet processing:

input:

```
StatusJSONImpl{createdAt=Thu Nov 01 11:45:07 CET 2018, id=1057946633147580418,
text=’RT @btsanalytics: "Waste It On Me" Steve Aoki (feat. @BTS_twt)
has now surpassed over...’}
```

output:

```
(2018/11/01 11:45, ’RT @btsanalytics: "Waste It On Me" Steve Aoki
(feat. @BTS_twt) has now surpassed...’)
```

The architecture is designed to handle multiple input streams from various sources, in form of multiple Kafka producers. 
However, since the amount of free streaming news APIs is limited, we've only used data from Twitter.

### 2. Reading from Kafka and Sentiment Analysis
A SparkSession is started and a keyspace and table are created in Cassandra. Data is then read from Kafka from the trends 
topic with the createDirectStream function. Sentiment analysis is computed on every tweet with the RNNCoreAnnotations model 
from Stanford’s NLP library. For each input String, the model outputs an integer between 0-4 where 0 is very negative, 1 is 
negative, 2 is neutral, 3 is positive and 4 is very positive. We accumulated 0 and 1 to a single negative class and 3 and 4 
to a single positive class. This resulted in the three final classes: positive, neutral and negative.

### 3. Counting Sentiments and Storing in Cassandra
The number of tweets predicted as positive, neutral and negative for each minute are counted and stored in the Cassandra 
table with four columns. The first column contains the key which is the tweet’s timestamp with minute precision. The three 
following columns contain the positive, neutral and negative counts for that minute. For every new minute, a new row is 
created in the table and counts for the three sentiments are accumulated as new tweets from that minute arrive.

### 4. Visualization
Data is retrieved from the Cassandra table every second and sorted by the timestamp. Data from the last 30 minutes are 
visualized in the web browser as a live bar plot created in Python with Plotpy and Dash. The number of positive, neutral 
and negative tweets for each minute are shown as stacked bars. A potential bottleneck is that the whole Cassandra table 
is retrieved every second and sorted according to the timestamp. As the program runs, the Cassandra table will become 
larger and the sorting will take longer time. We did not notice any delay from this in our experiments, however we only 
ran the program for at most an hour which creates in total 60 entries in the Cassandra table.

# To run the code

First start servers and create a topic:

> $KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
> $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties
> $KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic trends
> $CASSANDRA_HOME/bin/cassandra -f

where KAFKA_HOME and CASSANDRA_HOME are links to each directory.

To run the main files: compile TrendDetector.scala and TwitterInput.scala. Start TwitterInput.scala followed by 
TrendDetector.scala. We used an IDE (IntelliJ) in this project. Another soultion would be to set up an build.sbt and then run
> sbt run

To start the visualization, run the visualize.py file and go to 127.0.0.1:8050 in the web browser.
> python visualize.py

To see the results stored in database, open the cassandra cql shell and query the table.
> $CASSANDRA_HOME/bin/cqlsh
> select * from trend_space.trends;

Note: not all versions of the frameworks are compatible with each other. The following versions were used for this project:
* Spark: 2.2.1
* Scala: 2.11.8
* Cassandra: 3.11.2
* Kafka: 0.8.2.1
* Stanford NLP: 2018-10-05
* Twitter4j: 4.0.4

Furthermore spark-streaming-twitter, spark-streaming-kafka, spark-cassandra-connector are needed.

  

In the Cassandra table, we observed that event time and real-time were the same in the beginning when the program 
was started, but as the program runs for a couple of minutes, the event-time deviates more and more from real-time, 
resulting in a delay that is mainly caused by the sentiment analysis. If a popular keyword is chosen, the tweets are 
processed slower than they arrive and the accumulated delay became larger the longer we let the system run. If we choose 
a keyword with low frequency, for e.g. ”Verizon”, this problem is no longer noticeable. A potential solution to this 
problem is to parallelize the sentiment analysis. From our experiments, we also observed that an unproportionally 
large part of the tweets were predicted as negative by the RNNCoreAnnotations model. Depending on the choice of keyword, 
between 50% up to 90% of the tweets were predicted as negative by the model. Hence we suspect that the recurrent neural 
network model from Standford’s NLP library is biased towards negative. We do however not consider training a more accurate 
neural network as a part of this project as it is more of a machine learning/NLP problem.
