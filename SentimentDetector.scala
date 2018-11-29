package main
import java.util.Properties
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import scala.collection.convert.wrapAll._

/**
  * Sentiment Analysis Class that detects the sentiment of a text
  */
object SentimentDetector{
  val props = new Properties()
  props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
  props.setProperty("ssplit.isOneSentence","true")
  val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)

  // Use the Stanford NLP RNN model to predict the sentiment of the input text
  def getSentiment(input: String): String = {
    if (Option(input).isDefined && !input.trim.isEmpty) {
      val annotation = pipeline.process(input)
      val sentenceAnnotation = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
      val sentenceTree = sentenceAnnotation.map(text => (text, text.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
      val predicted = sentenceTree.map{case (text, tree) => (text.toString, convertToString(RNNCoreAnnotations.getPredictedClass(tree)))}
      val (_, sentiment) = predicted.maxBy{case (text, _) => text.length}
      sentiment
    } else{
      throw new IllegalArgumentException("Empty String!")
    }
  }

  // Readable output of sentiment
  def convertToString(sentiment: Int): String = sentiment match {
    case x if x == 0 || x == 1 => "NEGATIVE"
    case x if x == 3 || x == 4 => "POSITIVE"
    case 2 => "NEUTRAL"
  }
}