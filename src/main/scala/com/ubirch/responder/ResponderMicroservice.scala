package com.ubirch.responder

import java.nio.charset.StandardCharsets.UTF_8

import com.ubirch.niomon.base.NioMicroservice
import com.ubirch.kafka._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader

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
    // TODO: decide on what data should be included here
    //  also, the source of `record`s here is message-signer right now, so we lose the context - should something be
    //  done about that?
    record.toProducerRecord(
      topic = onlyOutputTopic,
      value = """{"message": "your request has been submitted"}""".getBytes(UTF_8)
    )
  }

  def handleError(topic: String, record: ConsumerRecord[String, Array[Byte]]): ProducerRecord[String, Array[Byte]] = {
    val headers = record.headersScala
    logger.debug(s"record headers: ${record.headersScala}")
    headers.get("http-status-code") match {
      // Special handling for unauthorized, because we don't handle that one via NioMicroservice error handling
      // (Q: maybe we should? but that would prevent us to easily do something with unauthorized, but otherwise valid
      // packets)
      case Some("401") => record.toProducerRecord(
        topic = onlyOutputTopic,
        value = stringifyException(UnauthorizedException, record.key()).getBytes(UTF_8)
      )
      case None =>
        logger.warn("Someone's not using NioMicroservice and forgot to attach `http-status-code` header to their " +
          s"error message. Message: [requestId = ${record.key()}, headers = $headers]")
        val httpStatusCodeHeader = new RecordHeader("http-status-code", "500".getBytes(UTF_8))
        record.toProducerRecord(
          topic = onlyOutputTopic,
          headers = (record.headers().toArray :+ httpStatusCodeHeader).toIterable.asJava
        )
      case _ => record.toProducerRecord(onlyOutputTopic)
    }
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

  object UnauthorizedException extends Exception("Unauthorized!")

}
