package com.ubirch.responder

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
import org.json4s.JsonAST.JString
import org.json4s.jackson.JsonMethods

import scala.collection.JavaConverters._
import scala.util.Try

class ResponderMicroservice(runtime: NioMicroservice[Either[String, MessageEnvelope], MessageEnvelope]) extends NioMicroserviceLogic(runtime) {
  // strings come from error topics, message envelopes from normal topics, see the routing in
  // `ResponderMicroservice.payloadFactory` below
  override def processRecord(record: ConsumerRecord[String, Either[String, MessageEnvelope]]): ProducerRecord[String, MessageEnvelope] = {
    record.value() match {
      case Right(envelope) => handleNormal(record.copy(value = envelope))
      case Left(string) => handleError(record.copy(value = string))
    }
  }

  private val normalUuid = UUID.fromString(config.getString("uuid.normal"))
  private val errorUuid = UUID.fromString(config.getString("uuid.error"))

  private val normalHint = 0
  private val errorHint = 0

  private final val payloadPadding = Array.fill[Byte](16)(0)

  def handleNormal(record: ConsumerRecord[String, MessageEnvelope]): ProducerRecord[String, MessageEnvelope] = {

    val tryResponseUPP = for {
      //We are adding 16 bytes in zeros to have a fixed payload of 32 bytes that
      //will allow clients to better read the signatures when ecdsa is applied with
      //raw signatures
      requestIdAsBytesWithPadding <- Try(record.requestIdHeader().get)
        .map(UUID.fromString)
        .map(UUIDUtil.uuidToBytes)
        .map(r => Array.concat(r, payloadPadding))

      requestUPP <- Try(record.value().ubirchPacket)
      payload <- Try(BinaryNode.valueOf(requestIdAsBytesWithPadding))

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
      record.toProducerRecord(
        topic = onlyOutputTopic,
        value = MessageEnvelope(upp)
      )
    }.getOrElse {
      handleError(record
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

  def handleError(record: ConsumerRecord[String, String]): ProducerRecord[String, MessageEnvelope] = {
    val headers = record.headersScala
    val requestId = record.requestIdHeader().orNull

    val previous = headers.CaseInsensitive.get("previous-microservice")
      .map(_.split("-")
        .toList
        .filter(_.nonEmpty)
        .flatMap(_.headOption.toList)
        .map(_.toUpper)
        .mkString("")
      ).getOrElse("NU")

    val xcodeHeader = headers.CaseInsensitive.get("x-code")
      .filter(_.nonEmpty)
      .map(x => "-" + x)
      .getOrElse("-0000")

    def xcode(status: String) = previous + status +  xcodeHeader

    def xpayload = Try(BinaryNode.valueOf(UUIDUtil.uuidToBytes(UUID.fromString(requestId))))

    logger.debug(s"record headers: $headers", v("requestId", requestId))

    headers.CaseInsensitive.get("http-status-code") match {
      // Special handling for unauthorized, because we don't handle that one via NioMicroservice error handling
      // (Q: maybe we should? but that would prevent us to easily do something with unauthorized, but otherwise valid
      // packets)
      case Some("401") =>

        val p = xpayload.getOrElse(errorPayload(stringifyException(UnauthorizedException, requestId)))
        val upp = new ProtocolMessage(ProtocolMessage.SIGNED, errorUuid, errorHint, p)

        record
          .toProducerRecord(topic = onlyOutputTopic, value = MessageEnvelope(upp))
          .withExtraHeaders("x-err"-> xcode("401"))

      case Some(status) =>

        val p = xpayload.getOrElse(errorPayload(record.value()))
        val upp = new ProtocolMessage(ProtocolMessage.SIGNED, errorUuid, errorHint, p)

        record
          .toProducerRecord(onlyOutputTopic, value = MessageEnvelope(upp))
          .withExtraHeaders("x-err"-> xcode(status))

      case None =>
        logger.warn("someone's not using NioMicroservice and forgot to attach `http-status-code` header to their " +
          s"error message. Message: [requestId = {}, headers = {}]", v("requestId", requestId), v("headers", headers.asJava))

        val p = xpayload.getOrElse(errorPayload(record.value()))
        val upp = new ProtocolMessage(ProtocolMessage.SIGNED, errorUuid, errorHint, p)

        record
          .toProducerRecord(onlyOutputTopic, value = MessageEnvelope(upp))
          .withExtraHeaders("http-status-code" -> "500", "x-err"-> xcode("500"))

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

  case object UnauthorizedException extends Exception("Unauthorized!")

  implicit val payloadFactory: KafkaPayloadFactory[Either[String, MessageEnvelope]] = { context =>
    val NormalTopic = topicMatcher(context.config.getStringList("normal-input-topics").asScala)
    val ErrorTopic = topicMatcher(context.config.getStringList("error-input-topics").asScala)

    KafkaPayload.topicBasedEitherKafkaPayload {
      case NormalTopic(_) => Right(())
      case ErrorTopic(_) => Left(())
    }
  }
}
