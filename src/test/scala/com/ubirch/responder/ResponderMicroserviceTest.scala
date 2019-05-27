package com.ubirch.responder

import java.util.UUID

import com.typesafe.config.ConfigFactory
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.niomon.base.NioMicroserviceMock
import com.ubirch.niomon.util.KafkaPayload
import com.ubirch.protocol.ProtocolMessage
import org.json4s.JsonAST.{JObject, JString}
import org.scalatest.{FlatSpec, Matchers}

//noinspection TypeAnnotation
class ResponderMicroserviceTest extends FlatSpec with Matchers {
  implicit val MESerializer = KafkaPayload[MessageEnvelope].serializer
  implicit val MEDeserializer = KafkaPayload[MessageEnvelope].deserializer

  import ResponderMicroservice.payloadFactory
  val microservice = NioMicroserviceMock(ResponderMicroservice(_))
  microservice.config = ConfigFactory.load().getConfig("responder")
  microservice.outputTopics = Map("default" -> "out")
  microservice.name = "responder"
  import microservice.kafkaMocks._


  "ResponderMicroservice" should "produce a valid MessageEnvelope" in {
    publishToKafka("success", MessageEnvelope(new ProtocolMessage()))
    val _ = consumeFirstMessageFrom[MessageEnvelope]("out")
  }

  it should "respond with the response from context, if the message is successful" in {
    publishToKafka("success", MessageEnvelope(new ProtocolMessage(), JObject("configuredResponse" -> JString("hello!"))))
    val res = consumeFirstMessageFrom[MessageEnvelope]("out")
    res.ubirchPacket.getPayload.asText() should equal ("hello!")
  }

  it should "use a default response if configuredResponse is missing from the context" in {
    publishToKafka("success", MessageEnvelope(new ProtocolMessage()))
    val res = consumeFirstMessageFrom[MessageEnvelope]("out")
    res.ubirchPacket.getPayload.toString should equal ("""{"message":"your request has been submitted"}""")
  }

  it should "accept non-MessageEnvelope messages on the error topic" in {
    publishStringMessageToKafka("error", microservice.stringifyException(new RuntimeException("foo"), "key"))
    val res = consumeFirstMessageFrom[MessageEnvelope]("out")
    res.ubirchPacket.getPayload.toString should equal ("""{"error":"RuntimeException: foo","causes":[],"microservice":"responder","requestId":"key"}""")
    res.ubirchPacket.getUUID should equal (UUID.fromString("deaddead-dead-dead-dead-deaddeaddead"))
  }
}
