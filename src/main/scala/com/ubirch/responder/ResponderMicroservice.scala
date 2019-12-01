package com.ubirch.responder

import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID

import com.fasterxml.jackson.databind.JsonNode
import com.ubirch.kafka._
import com.ubirch.niomon.base.{NioMicroservice, NioMicroserviceLogic}
import com.ubirch.niomon.util.{KafkaPayload, KafkaPayloadFactory}
import com.ubirch.protocol.ProtocolMessage
import com.ubirch.responder.ResponderMicroservice.UnauthorizedException
import net.logstash.logback.argument.StructuredArguments.v
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.json4s.JsonAST.{JString, JValue}
import org.json4s.jackson.JsonMethods

import scala.collection.JavaConverters._
import scala.util.Try

class ResponderMicroservice(runtime: NioMicroservice[Either[String, MessageEnvelope], MessageEnvelope]) extends NioMicroserviceLogic(runtime) {
  // strings come from error topics, message envelopes from normal topics, see the routing in
  // `ResponderMicroservice.payloadFactory` below
  override def processRecord(input: ConsumerRecord[String, Either[String, MessageEnvelope]]): ProducerRecord[String, MessageEnvelope] = {
    input.value() match {
      case Right(envelope) => handleNormal(input.copy(value = envelope))
      case Left(string) => handleError(input.copy(value = string))
    }
  }

  private val normalUuid = UUID.fromString(config.getString("uuid.normal"))
  private val errorUuid = UUID.fromString(config.getString("uuid.error"))

  private val normalHint = 0
  private val errorHint = 0

  def handleNormal(record: ConsumerRecord[String, MessageEnvelope]): ProducerRecord[String, MessageEnvelope] = {
    val response: JValue = Try(record.value().getContext[JValue]("configuredResponse"))
      .getOrElse(JsonMethods.parse("""{"message": "your request has been submitted"}"""))

    // nobody's gonna use this packet in this service, so we can recycle it for our purposes
    val upp = record.value().ubirchPacket
    upp.setHint(normalHint)
    upp.setUUID(normalUuid)
    upp.setPayload(JsonMethods.asJsonNode(response))

    record.toProducerRecord(
      topic = onlyOutputTopic,
      value = MessageEnvelope(upp)
    )
  }

  /** Tries to parse json and if that fails, represents input as json string */
  private def errorPayload(raw: String): JsonNode = {
    Try(JsonMethods.asJsonNode(JsonMethods.parse(raw))).getOrElse {
      JsonMethods.asJsonNode(JString(raw))
    }
  }

  def handleError(record: ConsumerRecord[String, String]): ProducerRecord[String, MessageEnvelope] = {
    val headers = record.headersScala
    logger.debug(s"record headers: ${record.headersScala}", v("requestId", record.key()))
    headers.get("http-status-code") match {
      // Special handling for unauthorized, because we don't handle that one via NioMicroservice error handling
      // (Q: maybe we should? but that would prevent us to easily do something with unauthorized, but otherwise valid
      // packets)
      case Some("401") =>
        val p = errorPayload(stringifyException(UnauthorizedException, record.key()))
        val upp = new ProtocolMessage(ProtocolMessage.SIGNED, errorUuid, errorHint, p)
        record.toProducerRecord(
          topic = onlyOutputTopic,
          value = MessageEnvelope(upp)
        )
      case None =>
        logger.warn("someone's not using NioMicroservice and forgot to attach `http-status-code` header to their " +
          s"error message. Message: [requestId = {}, headers = {}]", v("requestId", record.key()), v("headers", headers.asJava))

        val p = errorPayload(record.value())
        val upp = new ProtocolMessage(ProtocolMessage.SIGNED, errorUuid, errorHint, p)
        val httpStatusCodeHeader = new RecordHeader("http-status-code", "500".getBytes(UTF_8))
        record.toProducerRecord(
          topic = onlyOutputTopic,
          headers = (record.headers().toArray :+ httpStatusCodeHeader).toIterable.asJava,
          value = MessageEnvelope(upp)
        )
      case _ =>
        val p = errorPayload(record.value())
        val upp = new ProtocolMessage(ProtocolMessage.SIGNED, errorUuid, errorHint, p)
        record.toProducerRecord(onlyOutputTopic, value = MessageEnvelope(upp))
    }
  }
}

object ResponderMicroservice {
  def apply(runtime: NioMicroservice[Either[String, MessageEnvelope], MessageEnvelope]): ResponderMicroservice =
    new ResponderMicroservice(runtime)

  /** helper to match a string against set contents using case syntax */
  private class TopicSet(set: Set[String]) {
    def unapply(x: String): Option[String] = Some(x).filter(set.contains)
  }

  private def topicMatcher(topics: Iterable[String]) = {
    val set = topics.toSet
    new TopicSet(set)
  }

  object UnauthorizedException extends Exception("Unauthorized!")

  implicit val payloadFactory: KafkaPayloadFactory[Either[String, MessageEnvelope]] = { context =>
    val NormalTopic = topicMatcher(context.config.getStringList("normal-input-topics").asScala)
    val ErrorTopic = topicMatcher(context.config.getStringList("error-input-topics").asScala)

    KafkaPayload.topicBasedEitherKafkaPayload {
      case NormalTopic(_) => Right(())
      case ErrorTopic(_) => Left(())
    }
  }
}
