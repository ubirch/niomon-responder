package com.ubirch.responder

import java.util.{Base64, UUID}

import com.typesafe.config.ConfigFactory
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.niomon.base.NioMicroserviceMock
import com.ubirch.niomon.util.KafkaPayload
import com.ubirch.protocol.ProtocolMessage
import com.ubirch.protocol.codec.UUIDUtil
import org.apache.kafka.clients.producer.ProducerRecord
import org.json4s.JsonAST.{JObject, JString}
import org.scalatest.{FlatSpec, Matchers}

//noinspection TypeAnnotation
class ResponderMicroserviceTest extends FlatSpec with Matchers {
  implicit val MESerializer = KafkaPayload[MessageEnvelope].serializer
  implicit val MEDeserializer = KafkaPayload[MessageEnvelope].deserializer

  import ResponderMicroservice.payloadFactory
  val microservice = NioMicroserviceMock(ResponderMicroservice(_))
  microservice.config = ConfigFactory.load().getConfig("niomon-responder")
  microservice.outputTopics = Map("default" -> "out")
  microservice.name = "niomon-responder"
  import microservice.kafkaMocks._


  "ResponderMicroservice" should "produce a valid MessageEnvelope" in {
    publishToKafka("success", MessageEnvelope(new ProtocolMessage()))
    val _ = consumeFirstMessageFrom[MessageEnvelope]("out")
  }

  it should "respond with the request id, if the message is successful and request id has been set" in {
    val requestId = UUID.randomUUID()
    val pr = new ProducerRecord[String, MessageEnvelope]("success", MessageEnvelope(new ProtocolMessage(), JObject("configuredResponse" -> JString("hello!"))))
      .withRequestIdHeader()(requestId.toString)
    publishToKafka(pr)
    val res = consumeFirstMessageFrom[MessageEnvelope]("out")
    //We have to decode it as the helper here to get the payload returns a JsonNode and not a BinaryNode
    val returnedUUID = UUIDUtil.bytesToUUID(Base64.getDecoder.decode(res.ubirchPacket.getPayload.asText()))
    returnedUUID should equal (requestId)
  }

  it should "use a default response if configuredResponse is missing from the context" in {
    val requestId = UUID.randomUUID()
    publishToKafka(new ProducerRecord[String, MessageEnvelope]("success", MessageEnvelope(new ProtocolMessage())).withRequestIdHeader()(requestId.toString))
    val res = consumeFirstMessageFrom[MessageEnvelope]("out")
    //We have to decode it as the helper here to get the payload returns a JsonNode and not a BinaryNode
    val returnedUUID = UUIDUtil.bytesToUUID(Base64.getDecoder.decode(res.ubirchPacket.getPayload.asText()))
    returnedUUID should equal (requestId)
  }

  it should "accept non-MessageEnvelope messages on the error topic" in {
    publishStringMessageToKafka("error", microservice.stringifyException(new RuntimeException("foo"), "key"))
    val res = consumeFirstMessageFrom[MessageEnvelope]("out")
    res.ubirchPacket.getPayload.toString should equal ("""{"error":"RuntimeException: foo","causes":[],"microservice":"niomon-responder","requestId":"key"}""")
    res.ubirchPacket.getUUID should equal (UUID.fromString("deaddead-dead-dead-dead-deaddeaddead"))
  }
}
