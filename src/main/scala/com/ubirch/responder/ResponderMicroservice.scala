package com.ubirch.responder

import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.BinaryNode
import com.ubirch.kafka._
import com.ubirch.niomon.base.{NioMicroservice, NioMicroserviceLogic}
import com.ubirch.niomon.util.EnrichedMap.toEnrichedMap
import com.ubirch.niomon.util.{KafkaPayload, KafkaPayloadFactory}
import com.ubirch.protocol.ProtocolMessage
import com.ubirch.protocol.codec.UUIDUtil
import com.ubirch.responder.ResponderMicroservice.UnauthorizedException
import net.logstash.logback.argument.StructuredArguments.v
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.json4s.JsonAST.JString
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

  def handleNormal(input: ConsumerRecord[String, MessageEnvelope]): ProducerRecord[String, MessageEnvelope] = {

    val tryResponseUPP = for {
      requestId <- Try(input.requestIdHeader().get)
      requestUPP <- Try(input.value().ubirchPacket)
      payload <- Try(BinaryNode.valueOf(UUIDUtil.uuidToBytes(UUID.fromString(requestId))))
    } yield {
      val responseUPP = new ProtocolMessage()
      responseUPP.setVersion(requestUPP.getVersion)
      responseUPP.setUUID(normalUuid)
      responseUPP.setChain(requestUPP.getSignature)
      responseUPP.setHint(normalHint)
      responseUPP.setPayload(payload)
      responseUPP
    }

    tryResponseUPP.map { upp =>
      input.toProducerRecord(
        topic = onlyOutputTopic,
        value = MessageEnvelope(upp)
      )
    }.getOrElse {
      handleError(input
        .withExtraHeaders("http-status-code" -> "500")
        .copy(value = "Error creating response, but it is likely the message was accepted.")
      )
    }

  }

  /** Tries to parse json and if that fails, represents input as json string */
  private def errorPayload(raw: String): JsonNode = {
    Try(JsonMethods.asJsonNode(JsonMethods.parse(raw))).getOrElse {
      JsonMethods.asJsonNode(JString(raw))
    }
  }

  def handleError(input: ConsumerRecord[String, String]): ProducerRecord[String, MessageEnvelope] = {
    val headers = input.headersScala
    val requestId = input.requestIdHeader().orNull

    logger.debug(s"record headers: $headers", v("requestId", requestId))

    headers.CaseInsensitive.get("http-status-code") match {
      // Special handling for unauthorized, because we don't handle that one via NioMicroservice error handling
      // (Q: maybe we should? but that would prevent us to easily do something with unauthorized, but otherwise valid
      // packets)
      case Some("401") =>
        val p = errorPayload(stringifyException(UnauthorizedException, requestId))
        val upp = new ProtocolMessage(ProtocolMessage.SIGNED, errorUuid, errorHint, p)
        input.toProducerRecord(
          topic = onlyOutputTopic,
          value = MessageEnvelope(upp)
        )
      case None =>
        logger.warn("someone's not using NioMicroservice and forgot to attach `http-status-code` header to their " +
          s"error message. Message: [requestId = {}, headers = {}]", v("requestId", requestId), v("headers", headers.asJava))

        val p = errorPayload(input.value())
        val upp = new ProtocolMessage(ProtocolMessage.SIGNED, errorUuid, errorHint, p)
        val httpStatusCodeHeader = new RecordHeader("http-status-code", "500".getBytes(UTF_8))
        input.toProducerRecord(
          topic = onlyOutputTopic,
          headers = (input.headers().toArray :+ httpStatusCodeHeader).toIterable.asJava,
          value = MessageEnvelope(upp)
        )
      case _ =>
        val p = errorPayload(input.value())
        val upp = new ProtocolMessage(ProtocolMessage.SIGNED, errorUuid, errorHint, p)
        input.toProducerRecord(onlyOutputTopic, value = MessageEnvelope(upp))
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
