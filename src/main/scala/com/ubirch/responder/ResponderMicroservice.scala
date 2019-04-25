package com.ubirch.responder

import com.ubirch.niomon.base.NioMicroservice
import com.ubirch.kafka._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.JavaConverters._

class ResponderMicroservice extends NioMicroservice[Array[Byte], Array[Byte]]("responder") {
  import ResponderMicroservice._
  private val NormalTopic = topicSet(config.getStringList("normal-input-topics").asScala)
  private val ErrorTopic = topicSet(config.getStringList("error-input-topics").asScala)

  override def processRecord(input: ConsumerRecord[String, Array[Byte]]): ProducerRecord[String, Array[Byte]] = {
    input.topic() match {
      case NormalTopic(topic) => handleNormal(topic, input)
      case ErrorTopic(topic) => handleError(topic, input)
    }
  }

  def handleNormal(topic: String, record: ConsumerRecord[String, Array[Byte]]): ProducerRecord[String, Array[Byte]] = {
    record.toProducerRecord(onlyOutputTopic)
  }

  def handleError(topic: String, record: ConsumerRecord[String, Array[Byte]]): ProducerRecord[String, Array[Byte]] = {
    record.toProducerRecord(onlyOutputTopic)
  }
}

object ResponderMicroservice {
  private def topicSet(topics: Iterable[String]) = {
    val set = topics.toSet
    object TopicSet {
      def unapply(x: String): Option[String] = Some(x).filter(set.contains)
    }

    TopicSet
  }
}
